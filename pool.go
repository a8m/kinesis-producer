package producer

type TaskPool struct {
	nWorkers int
	tasks    chan func()
	done     chan struct{}
}

// NewTask gets conncurrency arguments, and spawn `n` workers
// that loop forever, consume and executing the incoming tasks
func newPool(conn int) *TaskPool {
	p := &TaskPool{
		nWorkers: conn,
		tasks:    make(chan func()),
		done:     make(chan struct{}),
	}
	return p
}

func (p *TaskPool) Start() {
	for i := 0; i < p.nWorkers; i++ {
		go worker(p.tasks, p.done)
	}
}

func (p *TaskPool) Put(t func()) {
	p.tasks <- t
}

func (p *TaskPool) Stop() {
	close(p.tasks)
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
