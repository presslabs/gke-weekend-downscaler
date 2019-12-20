package downscaler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/araddon/dateparse"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"

	gkev1 "google.golang.org/api/container/v1"
	"path"
)

const (
	nodePoolLabel          = "cloud.google.com/gke-nodepool"
	weekendDownscalerTaint = "gke-weekend-downscaler"
)

type nodePoolInfo struct {
	Autoscaling bool  `json:"autoscalingEnabled"`
	MaxNodes    int64 `json:"autoscalingMaxNodes"`
	MinNodes    int64 `json:"autoscalingMinNodes"`
	NumNodes    int   `json:"numNodes"`
}

type clusterConfig struct {
	DisabledUntil  time.Time
	ScaleUpAfter   time.Time
	ScaleDownAfter time.Time
}

type cluster struct {
	FQName     string
	Cluster    *gkev1.Cluster
	RESTConfig *rest.Config
	client     client.Client
	gkeClient  *gkev1.Service

	cfg clusterConfig

	dryRun bool
}

func NewCluster(fqName string, gkeCluster *gkev1.Cluster, dryRun bool) (*cluster, error) {
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
		AuthProvider: &clientcmdapi.AuthProviderConfig{Name: googleAuthPlugin},
	}

	c := &cluster{
		dryRun:     dryRun,
		FQName:     fqName,
		Cluster:    gkeCluster,
		RESTConfig: &cfg,
	}

	c.gkeClient, err = gkev1.NewService(context.Background())
	if err != nil {
		return nil, err
	}

	c.client, err = client.New(c.RESTConfig, client.Options{})
	if err != nil {
		return nil, err
	}

	c.cfg, err = c.loadConfig()
	if err != nil {
		return nil, err
	}

	return c, nil
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
	nodeList := &corev1.NodeList{}
	err := c.client.List(context.Background(), nodeList, client.MatchingLabels{nodePoolLabel: pool.Name})
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
	projectName := path.Base(path.Dir(c.FQName))
	name := fmt.Sprintf("projects/%s/locations/%s/clusters/%s/nodePools/%s", projectName, c.Cluster.Location, c.Cluster.Name, pool.Name)
	return name
}

func (c *cluster) disableAutoscaling(pool *gkev1.NodePool) error {
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
	if !c.dryRun {
		op, err := c.gkeClient.Projects.Locations.Clusters.NodePools.SetAutoscaling(c.getNodePoolID(pool), autoscaling).Do()
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

func (c *cluster) reenableAutoscaling(pool *gkev1.NodePool, poolInfo nodePoolInfo) error {
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
	if !c.dryRun {
		op, err := c.gkeClient.Projects.Locations.Clusters.NodePools.SetAutoscaling(c.getNodePoolID(pool), autoscaling).Do()
		if err != nil {
			return err
		}
		err = c.waitForOperation(op, 1800*time.Second, "DONE")
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) scaleDownPool(pool *gkev1.NodePool) error {
	log.Printf("[%s] setting %s node pool size to 0", c.FQName, pool.Name)
	if !c.dryRun {
		size := &gkev1.SetNodePoolSizeRequest{NodeCount: 0}
		op, err := c.gkeClient.Projects.Locations.Clusters.NodePools.SetSize(c.getNodePoolID(pool), size).Do()
		if err != nil {
			return err
		}
		err = c.waitForOperation(op, 1800*time.Second, "DONE")
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *cluster) scaleUpPool(pool *gkev1.NodePool, poolInfo nodePoolInfo) error {
	log.Printf("[%s] setting %s node pool size to %d", c.FQName, pool.Name, poolInfo.NumNodes)
	if !c.dryRun {
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

func (c *cluster) evictNodes() error {
	nodes := &corev1.NodeList{}
	err := c.client.List(context.TODO(), nodes)
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
		if !c.dryRun {
			err := c.client.Patch(context.TODO(), &node, patch)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func getTimeFromConfig(cfg map[string]string, key string) (time.Time, error) {
	if cfg == nil {
		return time.Time{}, nil
	}

	t, exists := cfg[key]
	if !exists {
		return time.Time{}, nil
	}

	return dateparse.ParseStrict(t)
}

func (c *cluster) loadConfig() (clusterConfig, error) {
	clusterCfg := clusterConfig{}
	cfg := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "gke-weekend-downscaler",
		},
	}

	key, _ := client.ObjectKeyFromObject(cfg)
	err := c.client.Get(context.TODO(), key, cfg)
	if client.IgnoreNotFound(err) != nil {
		return clusterCfg, err
	}

	clusterCfg.DisabledUntil, err = getTimeFromConfig(cfg.Data, "disabled-until")
	if err != nil {
		return clusterCfg, err
	}
	clusterCfg.ScaleUpAfter, err = getTimeFromConfig(cfg.Data, "scale-up-after")
	if err != nil {
		return clusterCfg, err
	}
	clusterCfg.ScaleDownAfter, err = getTimeFromConfig(cfg.Data, "scale-down-after")
	if err != nil {
		return clusterCfg, err
	}
	return clusterCfg, nil
}

func (c *cluster) ScaleDown() error {
	now := time.Now()
	if c.cfg.DisabledUntil.After(now) {
		log.Printf("[%s] scale down disabled until %s by disabled-until config. skipping", c.FQName, c.cfg.DisabledUntil)
		return nil
	}
	if c.cfg.ScaleDownAfter.After(now) {
		log.Printf("[%s] scale down disabled until %s by scale-down-after config. skipping", c.FQName, c.cfg.ScaleDownAfter)
		return nil
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
	err = c.client.Get(context.TODO(), poolCfgKey, poolCfg)
	if client.IgnoreNotFound(err) != nil {
		return err
	}

	if err == nil { // gke-weekend-downscaler-pool-config exists, let's check the node count
		nodes := corev1.NodeList{}
		err := c.client.List(context.TODO(), &nodes)
		if client.IgnoreNotFound(err) != nil {
			return err
		}
		if len(nodes.Items) != 0 {
			return fmt.Errorf("[%s] kube-system/gke-weekend-downscaler-pool-config exists but %d nodes still exist", c.FQName, len(nodes.Items))
		} else {
			log.Printf("[%s] cluster already scaled down. skipping", c.FQName)
			return nil
		}
	}

	log.Printf("[%s] creating configmap %s", c.FQName, poolCfgKey)
	if !c.dryRun {
		err = c.client.Create(context.TODO(), poolCfg)
		if err != nil {
			return err
		}
	}

	// TODO: wait for previously started cluster operations before proceeding

	for _, pool := range c.Cluster.NodePools {
		if err := c.disableAutoscaling(pool); err != nil {
			return err
		}
	}

	if err := c.evictNodes(); err != nil {
		return err
	}

	for _, pool := range c.Cluster.NodePools {
		if err := c.scaleDownPool(pool); err != nil {
			return err
		}
	}

	return nil
}

func (c *cluster) ScaleUp() error {
	now := time.Now()
	if c.cfg.DisabledUntil.After(now) {
		log.Printf("[%s] scale up disabled until %s by disabled-until config. skipping", c.FQName, c.cfg.DisabledUntil)
		return nil
	}

	if c.cfg.ScaleUpAfter.After(now) {
		log.Printf("[%s] scale up disabled until %s by scale-down-after config. skipping", c.FQName, c.cfg.ScaleUpAfter)
		return nil
	}

	poolCfg := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "gke-weekend-autoscaler-pool-config",
		},
	}
	poolCfgKey, _ := client.ObjectKeyFromObject(poolCfg)
	err := c.client.Get(context.TODO(), poolCfgKey, poolCfg)
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
		if err := c.scaleUpPool(pool, poolInfo); err != nil {
			return err
		}
		if err := c.reenableAutoscaling(pool, poolInfo); err != nil {
			return err
		}
	}

	log.Printf("[%s] removing configmap %s", c.FQName, poolCfgKey)
	if !c.dryRun {
		return c.client.Delete(context.TODO(), poolCfg)
	}

	return nil
}
