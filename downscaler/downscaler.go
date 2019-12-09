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
		cl, err := NewCluster(fqName, c, w.dryRun)
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

func (w *weekendDownscaler) ScaleUp() error {
	clusters, err := w.getClusters()
	if err != nil {
		return err
	}
	for _, c := range clusters {
		if w.match(c.FQName) {
			if w.dryRun {
				log.Printf("[%s] scaling up cluster [dry-run]", c.FQName)
			} else {
				log.Printf("[%s] scaling up cluster", c.FQName)
			}
			err := c.ScaleUp()
			if err != nil {
				return err
			}
			log.Printf("[%s] successfully scaled up cluster", c.FQName)
		} else {
			log.Printf("[%s] skipping cluster", c.FQName)
		}
	}
	return nil
}

func (w *weekendDownscaler) ScaleDown() error {
	clusters, err := w.getClusters()
	if err != nil {
		return err
	}
	for _, c := range clusters {
		if w.match(c.FQName) {
			if w.dryRun {
				log.Printf("[%s] scaling down cluster [dry-run]", c.FQName)
			} else {
				log.Printf("[%s] scaling down cluster", c.FQName)
			}
			err := c.ScaleDown()
			if err != nil {
				return err
			}
			log.Printf("[%s] successfully scaled down cluster", c.FQName)
		} else {
			log.Printf("[%s] skipping cluster", c.FQName)
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
