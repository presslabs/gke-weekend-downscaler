package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"

	resourcemanagerv1 "google.golang.org/api/cloudresourcemanager/v1"
	resourcemanagerv2 "google.golang.org/api/cloudresourcemanager/v2"
	gkev1 "google.golang.org/api/container/v1"

	"github.com/gobwas/glob"

	wcluster "github.com/presslabs/gke-weekend-downscaler/cluster"
)

const workers = 4

var (
	DryRun bool
	Globs  []string
)

var Source string

const (
	nodePoolLabel          = "cloud.google.com/gke-nodepool"
	weekendDownscalerTaint = "gke-weekend-downscaler"
)

type weekendDownscaler struct {
	numWorkers              int
	gkeClient               *gkev1.Service
	resourceManagerClient   *resourcemanagerv1.Service
	resourceManagerV2Client *resourcemanagerv2.Service

	folderCache map[string]*resourcemanagerv2.Folder

	globs []glob.Glob
}

type cluster struct {
	FQName     string
	Cluster    *gkev1.Cluster
	RESTConfig *rest.Config
	client     client.Client
	gkeClient  *gkev1.Service
}

type nodePoolInfo struct {
	Autoscaling bool  `json:"autoscalingEnabled"`
	MaxNodes    int64 `json:"autoscalingMaxNodes"`
	MinNodes    int64 `json:"autoscalingMinNodes"`
	NumNodes    int   `json:"numNodes"`
}

func NewCluster(fqName string, gkeCluster *gkev1.Cluster) (*cluster, error) {
	var err error

	caData, err := base64.StdEncoding.DecodeString(gkeCluster.MasterAuth.ClusterCaCertificate)
	if err != nil {
		return nil, err
	}

	cfg := rest.Config{
		Host: gkeCluster.Endpoint,
		TLSClientConfig: rest.TLSClientConfig{
			CAData: caData,
		},
		AuthProvider: &clientcmdapi.AuthProviderConfig{Name: wcluster.GoogleAuthPlugin},
	}

	c := &cluster{
		FQName:     fqName,
		Cluster:    gkeCluster,
		RESTConfig: &cfg,
	}

	c.gkeClient, err = gkev1.NewService(context.Background())
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *cluster) GetClient() (client.Client, error) {
	if c.client != nil {
		return c.client, nil
	}

	cl, err := client.New(c.RESTConfig, client.Options{})
	if err != nil {
		return nil, err
	}
	c.client = cl
	return c.client, nil
}

func (c *cluster) waitForOperation(op *gkev1.Operation, timeout time.Duration, status string) error {
	timeoutC := time.After(timeout)
	tickC := time.Tick(5 * time.Second)

	operationID := strings.TrimPrefix(op.SelfLink, c.gkeClient.BasePath+"v1/")

	op, err := c.gkeClient.Projects.Locations.Operations.Get(operationID).Do()
	if err != nil {
		return err
	}
	log.Printf("[%s] %s (%s) is %s [timeout=%s]", c.FQName, op.Name, op.OperationType, op.Status, timeout)
	if op.Status == status {
		return nil
	}

	for {
		select {
		case <-timeoutC:
			return fmt.Errorf("timeout while waiting for %s to become %s [timeout=%s]", op.Name, status, timeout)
		case <-tickC:
			op, err := c.gkeClient.Projects.Locations.Operations.Get(operationID).Do()
			if err != nil {
				return err
			}
			log.Printf("[%s] %s (%s) is %s [timeout=%s]", c.FQName, op.Name, op.OperationType, op.Status, timeout)
			if op.Status == status {
				return nil
			}
		}
	}
}

func (c *cluster) nodePoolInfo(pool *gkev1.NodePool) (nodePoolInfo, error) {
	info := nodePoolInfo{}
	cl, err := c.GetClient()
	if err != nil {
		return info, err
	}
	nodeList := &corev1.NodeList{}
	err = cl.List(context.Background(), nodeList, client.MatchingLabels{nodePoolLabel: pool.Name})
	if err != nil {
		return info, err
	}

	info.Autoscaling = pool.Autoscaling.Enabled
	info.MaxNodes = pool.Autoscaling.MaxNodeCount
	info.MinNodes = pool.Autoscaling.MinNodeCount
	info.NumNodes = len(nodeList.Items)
	return info, nil
}

func (c *cluster) getNodePoolID(pool *gkev1.NodePool) string {
	return strings.TrimPrefix(pool.SelfLink, c.gkeClient.BasePath+"v1/")
}

func (c *cluster) disableAutoscaling(dryRun bool, pool *gkev1.NodePool) error {
	if !pool.Autoscaling.Enabled {
		log.Printf("[%s] autoscaling is already disbled for %s node pool", c.FQName, pool.Name)
		return nil
	}
	autoscaling := &gkev1.SetNodePoolAutoscalingRequest{
		Autoscaling: &gkev1.NodePoolAutoscaling{
			Enabled: false,
		},
	}

	log.Printf("[%s] disabling autoscaling for %s node pool", c.FQName, pool.Name)
	if !dryRun {
		op, err := c.gkeClient.Projects.Locations.Clusters.NodePools.SetAutoscaling(c.getNodePoolID(pool), autoscaling).Do()
		if err != nil {
			return err
		}
		err = c.waitForOperation(op, 300*time.Second, "DONE")
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) reenableAutoscaling(dryRun bool, pool *gkev1.NodePool, poolInfo nodePoolInfo) error {
	if !poolInfo.Autoscaling {
		log.Printf("[%s] skip reenableing autoscaling for %s node pool as it was previously disabled", c.FQName, pool.Name)
		return nil
	}
	autoscaling := &gkev1.SetNodePoolAutoscalingRequest{
		Autoscaling: &gkev1.NodePoolAutoscaling{
			Enabled:      poolInfo.Autoscaling,
			MaxNodeCount: poolInfo.MaxNodes,
			MinNodeCount: poolInfo.MinNodes,
		},
	}

	log.Printf("[%s] reenableing autoscaling for %s node pool (%d-%d nodes)", c.FQName, pool.Name, poolInfo.MinNodes, poolInfo.MaxNodes)
	if !dryRun {
		op, err := c.gkeClient.Projects.Locations.Clusters.NodePools.SetAutoscaling(c.getNodePoolID(pool), autoscaling).Do()
		if err != nil {
			return err
		}
		err = c.waitForOperation(op, 300*time.Second, "DONE")
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) scaleDownPool(dryRun bool, pool *gkev1.NodePool) error {
	log.Printf("[%s] setting %s node pool size to 0", c.FQName, pool.Name)
	if !dryRun {
		size := &gkev1.SetNodePoolSizeRequest{NodeCount: 0}
		op, err := c.gkeClient.Projects.Locations.Clusters.NodePools.SetSize(c.getNodePoolID(pool), size).Do()
		if err != nil {
			return err
		}
		err = c.waitForOperation(op, 900*time.Second, "DONE")
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *cluster) scaleUpPool(dryRun bool, pool *gkev1.NodePool, poolInfo nodePoolInfo) error {
	log.Printf("[%s] setting %s node pool size to %d", c.FQName, pool.Name, poolInfo.NumNodes)
	if !dryRun {
		size := &gkev1.SetNodePoolSizeRequest{NodeCount: int64(poolInfo.NumNodes)}
		op, err := c.gkeClient.Projects.Locations.Clusters.NodePools.SetSize(c.getNodePoolID(pool), size).Do()
		if err != nil {
			return err
		}
		err = c.waitForOperation(op, 900*time.Second, "DONE")
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *cluster) nodePoolsInfo() (map[string]nodePoolInfo, error) {
	info := make(map[string]nodePoolInfo)
	for _, pool := range c.Cluster.NodePools {
		poolInfo, err := c.nodePoolInfo(pool)
		if err != nil {
			return nil, err
		}
		info[pool.Name] = poolInfo
	}
	return info, nil
}

func upsertTaint(taint corev1.Taint, taints []corev1.Taint) []corev1.Taint {
	for i := range taints {
		if taints[i].Key == taint.Key && taints[i].Value == taint.Value {
			taints[i].Effect = taint.Effect
		}
	}
	return append(taints, taint)
}

func removeTaint(taint corev1.Taint, taints []corev1.Taint) []corev1.Taint {
	r := taints[:0]
	for _, t := range taints {
		if t.Key != taint.Key || t.Value != taint.Value || t.Effect != taint.Effect {
			r = append(r, t)
		}
	}
	return r
}

func (c *cluster) evictNodes(dryRun bool) error {
	cl, err := c.GetClient()
	if err != nil {
		return err
	}
	nodes := &corev1.NodeList{}
	err = cl.List(context.TODO(), nodes)
	if err != nil {
		return err
	}
	for _, node := range nodes.Items {
		origNode := node.DeepCopy()
		patch := client.MergeFrom(origNode)
		node.Spec.Unschedulable = true
		node.Spec.Taints = upsertTaint(corev1.Taint{
			Key:    weekendDownscalerTaint,
			Value:  "true",
			Effect: corev1.TaintEffectNoExecute,
		}, node.Spec.Taints)
		log.Printf("[%s] evicting node %s", c.FQName, node.Name)
		if !dryRun {
			err := cl.Patch(context.TODO(), &node, patch)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *cluster) ScaleDown(dryRun bool) error {
	cl, err := c.GetClient()
	if err != nil {
		return err
	}

	poolsInfo, err := c.nodePoolsInfo()
	if err != nil {
		return err
	}
	poolsInfoJSON, err := json.Marshal(poolsInfo)
	if err != nil {
		return err
	}

	poolCfg := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "gke-weekend-autoscaler-pool-config",
		},
		Data: map[string]string{
			"pool-cfg.json": string(poolsInfoJSON),
		},
	}

	poolCfgKey, _ := client.ObjectKeyFromObject(poolCfg)
	log.Printf("[%s] creating configmap %s", c.FQName, poolCfgKey)
	if !dryRun {
		err = cl.Create(context.TODO(), poolCfg)
		if err != nil {
			return err
		}
	}

	// TODO: wait for previously started cluster operations before proceeding

	for _, pool := range c.Cluster.NodePools {
		if err := c.disableAutoscaling(dryRun, pool); err != nil {
			return err
		}
	}

	if err := c.evictNodes(dryRun); err != nil {
		return err
	}

	for _, pool := range c.Cluster.NodePools {
		if err := c.scaleDownPool(dryRun, pool); err != nil {
			return err
		}
	}

	return nil
}

func (c *cluster) ScaleUp(dryRun bool) error {
	cl, err := c.GetClient()
	if err != nil {
		return err
	}
	poolCfg := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "gke-weekend-autoscaler-pool-config",
		},
	}
	poolCfgKey, _ := client.ObjectKeyFromObject(poolCfg)
	err = cl.Get(context.TODO(), poolCfgKey, poolCfg)
	if client.IgnoreNotFound(err) != nil {
		return err
	} else if err != nil {
		// Weekend Downscaler previous config not found. Bailing out.
		log.Printf("[%s] configmap %s not found, bailing out", c.FQName, poolCfgKey)
		return nil
	}

	poolsInfoJSON, exists := poolCfg.Data["pool-cfg.json"]
	if !exists {
		log.Printf("[%s] configmap %s has no pool-cfg.json, bailing out", c.FQName, poolCfgKey)
		return nil
	}

	poolsInfo := make(map[string]nodePoolInfo)
	err = json.Unmarshal([]byte(poolsInfoJSON), &poolsInfo)
	if err != nil {
		return err
	}

	for _, pool := range c.Cluster.NodePools {
		poolInfo, exists := poolsInfo[pool.Name]
		if !exists {
			log.Printf("[%s] pool %s has no previous config, skipping", c.FQName, pool.Name)
		}
		if err := c.scaleUpPool(dryRun, pool, poolInfo); err != nil {
			return err
		}
		if err := c.reenableAutoscaling(dryRun, pool, poolInfo); err != nil {
			return err
		}
	}

	log.Printf("[%s] removing configmap %s", c.FQName, poolCfgKey)
	if !dryRun {
		return cl.Delete(context.TODO(), poolCfg)
	}

	return nil
}

func NewWeekendDownscaler(ctx context.Context, globs []string) (*weekendDownscaler, error) {
	var err error
	w := &weekendDownscaler{
		folderCache: make(map[string]*resourcemanagerv2.Folder),
		globs:       make([]glob.Glob, len(globs), len(globs)),
	}

	for i := range globs {
		w.globs[i] = glob.MustCompile(globs[i], '/')
	}

	w.gkeClient, err = gkev1.NewService(ctx)
	if err != nil {
		return nil, err
	}

	w.resourceManagerClient, err = resourcemanagerv1.NewService(ctx)
	if err != nil {
		return nil, err
	}

	w.resourceManagerV2Client, err = resourcemanagerv2.NewService(ctx)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func (w *weekendDownscaler) getParentPath(res *resourcemanagerv1.ResourceId) (string, error) {
	var err error
	if res == nil {
		return "", nil
	}

	switch res.Type {
	case "folder":
		folder, exists := w.folderCache[res.Id]
		if !exists {
			folder, err = w.resourceManagerV2Client.Folders.Get(path.Join("folders", res.Id)).Do()
			if err != nil {
				return "", err
			}
			w.folderCache[res.Id] = folder
		}
		return folder.Name, nil
	case "organization":
		return path.Join("organizations", res.Id), nil
	}
	return "", nil
}

func (w *weekendDownscaler) match(path string) bool {
	for _, g := range w.globs {
		if g.Match(path) {
			return true
		}
	}
	return false
}

func (w *weekendDownscaler) getProjects() ([]string, error) {
	l := []string{}
	nextToken := ""
	for {
		listCall := w.resourceManagerClient.Projects.List()
		if nextToken != "" {
			listCall.PageToken(nextToken)
		}
		projects, err := listCall.Do()
		if err != nil {
			return nil, err
		}
		nextToken = projects.NextPageToken
		for _, proj := range projects.Projects {
			parentPath, err := w.getParentPath(proj.Parent)
			if err != nil {
				return nil, err
			}
			p := path.Join(parentPath, proj.ProjectId)
			l = append(l, p)
		}
		if nextToken == "" {
			break
		}
	}
	return l, nil
}

func (w *weekendDownscaler) getClustersForProject(projectFQName string) ([]*cluster, error) {
	clusters := []*cluster{}
	_, projectId := path.Split(projectFQName)
	list, err := w.gkeClient.Projects.Locations.Clusters.List(fmt.Sprintf("projects/%s/locations/-", projectId)).Do()
	if err != nil {
		return nil, err
	}
	for _, c := range list.Clusters {
		fqName := path.Join(projectFQName, c.Name)
		cl, err := NewCluster(fqName, c)
		if err != nil {
			return nil, err
		}
		clusters = append(clusters, cl)
	}
	return clusters, nil
}

func (w *weekendDownscaler) getClusters() ([]*cluster, error) {
	// These calls take a lot of time, even for a small number of projects, so let's parallelize
	projectsC := make(chan string)
	done := make(chan struct{})
	clusters := []*cluster{}
	var wg sync.WaitGroup
	var mux sync.Mutex

	for i := 0; i < workers; i++ {
		go func(worker int) {
			wg.Add(1)
			defer wg.Done()

			for {
				select {
				case p := <-projectsC:
					cl, err := w.getClustersForProject(p)
					if err != nil {
						log.Fatal(err) // short-circuit here, for dev speed
					}
					mux.Lock()
					clusters = append(cl, clusters...)
					mux.Unlock()
				case <-done:
					return
				}
			}
		}(i)
	}

	projects, err := w.getProjects()
	if err != nil {
		return nil, err
	}
	for _, p := range projects {
		projectsC <- p
	}
	close(done)
	wg.Wait()

	return clusters, nil
}

func scaleDown(cmd *cobra.Command, args []string) {
	w, err := NewWeekendDownscaler(context.Background(), Globs)
	if err != nil {
		log.Fatal(err)
	}

	clusters, err := w.getClusters()
	if err != nil {
		log.Fatal(err)
	}
	for _, c := range clusters {
		if w.match(c.FQName) {
			if DryRun {
				log.Printf("[%s] scaling down cluster [dry-run]", c.FQName)
			} else {
				log.Printf("[%s] scaling down cluster", c.FQName)
			}
			err := c.ScaleDown(DryRun)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("[%s] successfully scaled down cluster", c.FQName)
		} else {
			log.Printf("[%s] skipping cluster", c.FQName)
		}
	}
}

func scaleUp(cmd *cobra.Command, args []string) {
	w, err := NewWeekendDownscaler(context.Background(), Globs)
	if err != nil {
		log.Fatal(err)
	}

	clusters, err := w.getClusters()
	if err != nil {
		log.Fatal(err)
	}
	for _, c := range clusters {
		if w.match(c.FQName) {
			if DryRun {
				log.Printf("[%s] scaling up cluster [dry-run]", c.FQName)
			} else {
				log.Printf("[%s] scaling up cluster", c.FQName)
			}
			err := c.ScaleUp(DryRun)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("[%s] successfully scaled up cluster", c.FQName)
		} else {
			log.Printf("[%s] skipping cluster", c.FQName)
		}
	}
}

func lsClusters(cmd *cobra.Command, args []string) {
	w, err := NewWeekendDownscaler(context.Background(), Globs)
	if err != nil {
		log.Fatal(err)
	}

	clusters, err := w.getClusters()
	if err != nil {
		log.Fatal(err)
	}
	for _, c := range clusters {
		if len(w.globs) == 0 || w.match(c.FQName) {
			fmt.Println(c.FQName)
		}
	}
}

var rootCmd = &cobra.Command{
	Use:   "gke-weekend-downscaler",
	Short: "GKE Weekend Downscaler downscales GKE clusers on weekends",
}

var scaleUpCmd = &cobra.Command{
	Use:   "scale-up",
	Short: "Scale up clusters matching globs",
	Run:   scaleUp,
}

var scaleDownCmd = &cobra.Command{
	Use:   "scale-down",
	Short: "Scale down clusters matching globs",
	Run:   scaleDown,
}

var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List clusters matching globs",
	Run:   lsClusters,
}

func foo() {
	w, err := NewWeekendDownscaler(context.Background(), Globs)
	if err != nil {
		log.Fatal(err)
	}

	clusters, err := w.getClusters()
	if err != nil {
		log.Fatal(err)
	}
	for _, c := range clusters {
		if w.match(c.FQName) {
			log.Printf("[%s] scaling down cluster", c.FQName)
			err := c.ScaleUp(os.Getenv("DRY_RUN") == "true")
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("[%s] successfully scaled down cluster", c.FQName)
		} else {
			log.Printf("[%s] skipping cluster", c.FQName)
		}
	}
}

func main() {
	rootCmd.PersistentFlags().BoolVarP(&DryRun, "dry-run", "n", false, "don't change anything")
	rootCmd.PersistentFlags().StringSliceVarP(&Globs, "match", "m", []string{}, "glob matching for clusters")
	rootCmd.AddCommand(scaleUpCmd)
	rootCmd.AddCommand(scaleDownCmd)
	rootCmd.AddCommand(lsCmd)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
