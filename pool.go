package producer

// TaskPool is a simple task queue implementation
type TaskPool struct {
	// Number of workers to spawn
	nWorkers int

	// incoming tasks channel; task is simply a closure.
	tasks chan func()

	// done used to gracefully `Close` the pool.
	done chan struct{}
}

// NewTask gets conncurrency argument, and returns new TaskPool.
// Should start manually.
func newPool(conn int) *TaskPool {
	p := &TaskPool{
		nWorkers: conn,
		tasks:    make(chan func()),
		done:     make(chan struct{}),
	}
	return p
}

// Start spawn `n` workers that loops forever, consume
// and executing the incoming tasks.
func (p *TaskPool) Start() {
	for i := 0; i < p.nWorkers; i++ {
		go worker(p.tasks, p.done)
	}
}

// Put may block the caller if there is no available worker
func (p *TaskPool) Put(t func()) {
	p.tasks <- t
}

// Stop will close all channels and goroutines managed by the pool.
// calling `Stop` is a blocking call that guarantees all goroutines
// are stopped.
func (p *TaskPool) Stop() {
	close(p.tasks)
	defer close(p.done)
	dones := 0
	for dones < p.nWorkers {
		_ = <-p.done
		dones++
	}
}

func worker(tasks <-chan func(), done chan<- struct{}) {
	for {
		task, ok := <-tasks
		if !ok {
			break
		}
		task()
	}
	done <- struct{}{}
}
