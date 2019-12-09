package downscaler

import (
	"sync"
)

type Job interface{}

type Workqueue struct {
	done chan struct{}
	jobs chan Job
	errs chan error
	wg   sync.WaitGroup
}

type WorkFunc func(Job) error

func NewWorkqueue(capacity, workers int, f WorkFunc) *Workqueue {
	wq := Workqueue{}
	wq.done = make(chan struct{})
	wq.jobs = make(chan Job, capacity)
	wq.errs = make(chan error, capacity)
	for i := 0; i < workers; i++ {
		wq.wg.Add(1)
		go func(w int, jobs <-chan Job) {
			defer wq.wg.Done()
			for {
				select {
				case job, received := <-wq.jobs:
					if !received {
						return
					}
					wq.errs <- f(job)
				case <-wq.done:
					return
				}
			}
		}(i, wq.jobs)
	}
	return &wq
}

func (wq *Workqueue) Enqueue(j Job) bool {
	select {
	case wq.jobs <- j:
		return true
	default:
		return false
	}
}

func (wq *Workqueue) Wait() {
	close(wq.jobs)
	wq.wg.Wait()
	close(wq.errs)
}

func (wq *Workqueue) Errors() chan error {
	return wq.errs
}
