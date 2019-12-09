package downscaler

import (
	"context"
	"fmt"
	"log"
	"path"
	"sync"

	resourcemanagerv1 "google.golang.org/api/cloudresourcemanager/v1"
	resourcemanagerv2 "google.golang.org/api/cloudresourcemanager/v2"
	gkev1 "google.golang.org/api/container/v1"

	"github.com/gobwas/glob"
)

const workers = 4

type weekendDownscaler struct {
	dryRun     bool
	numWorkers int

	gkeClient               *gkev1.Service
	resourceManagerClient   *resourcemanagerv1.Service
	resourceManagerV2Client *resourcemanagerv2.Service

	folderCache map[string]*resourcemanagerv2.Folder

	globs []glob.Glob
}

func New(ctx context.Context, globs []string, dryRun bool) (*weekendDownscaler, error) {
	var err error
	w := &weekendDownscaler{
		folderCache: make(map[string]*resourcemanagerv2.Folder),
		globs:       make([]glob.Glob, len(globs), len(globs)),
		dryRun:      dryRun,
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
				if IsForbidden(err) {
					log.Printf("cannot get parent resource folders/%s. skipping", res.Id)
					return "", nil
				}
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
		if IsForbidden(err) {
			log.Printf("listing clusters for %s is forbidden. skipping", projectFQName)
			return nil, nil
		} else {
			return nil, err
		}
	}
	for _, c := range list.Clusters {
		fqName := path.Join(projectFQName, c.Name)
		cl, err := NewCluster(fqName, c, w.dryRun)
		if err != nil {
			return nil, err
		}
		clusters = append(clusters, cl)
	}
	return clusters, nil
}

func (w *weekendDownscaler) getClusters() ([]*cluster, error) {
	var mux sync.Mutex
	clusters := []*cluster{}

	projects, err := w.getProjects()
	if err != nil {
		return nil, err
	}
	if len(projects) == 0 {
		return clusters, nil
	}

	wq := NewWorkqueue(len(projects), workers, func(j Job) error {
		p, ok := j.(string)
		if !ok {
			log.Fatalf("cannot convert %T to string", j)
		}
		cl, err := w.getClustersForProject(p)
		if err != nil {
			return err
		}
		mux.Lock()
		clusters = append(cl, clusters...)
		mux.Unlock()
		return nil
	})

	for _, p := range projects {
		if !wq.Enqueue(p) {
			return nil, fmt.Errorf("getClusters() queue full")
		}
	}
	wq.Wait()
	for err = range wq.Errors() {
		if err != nil {
			return nil, err
		}
	}
	return clusters, nil
}

func (w *weekendDownscaler) ScaleUp() error {
	clusters, err := w.getClusters()
	if err != nil {
		return err
	}
	wq := NewWorkqueue(len(clusters), workers, func(j Job) error {
		c, ok := j.(*cluster)
		if !ok {
			log.Fatalf("cannot convert %T to cluster", j)
		}
		if w.match(c.FQName) {
			log.Printf("[%s] [dry-run=%t] scaling up cluster", c.FQName, w.dryRun)
			err := c.ScaleUp()
			if err != nil {
				return err
			}
			log.Printf("[%s] [dry-run=%t] successfully scaled up cluster", c.FQName, w.dryRun)
		}
		return nil
	})

	for _, c := range clusters {
		if !wq.Enqueue(c) {
			return fmt.Errorf("ScaleUp() queue full")
		}
	}

	wq.Wait()
	for err = range wq.Errors() {
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *weekendDownscaler) ScaleDown() error {
	clusters, err := w.getClusters()
	if err != nil {
		return err
	}
	wq := NewWorkqueue(len(clusters), workers, func(j Job) error {
		c, ok := j.(*cluster)
		if !ok {
			log.Fatalf("cannot convert %T to cluster", j)
		}
		if w.match(c.FQName) {
			log.Printf("[%s] [dry-run=%t] scaling down cluster", c.FQName, w.dryRun)
			err := c.ScaleDown()
			if err != nil {
				return err
			}
			log.Printf("[%s] [dry-run=%t] successfully scaled down cluster", c.FQName, w.dryRun)
		}
		return nil
	})

	for _, c := range clusters {
		if !wq.Enqueue(c) {
			return fmt.Errorf("ScaleDown() queue full")
		}
	}

	wq.Wait()
	for err = range wq.Errors() {
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *weekendDownscaler) ListClusters() error {
	clusters, err := w.getClusters()
	if err != nil {
		return err
	}
	for _, c := range clusters {
		if len(w.globs) == 0 || w.match(c.FQName) {
			fmt.Println(c.FQName)
		}
	}
	return nil
}
