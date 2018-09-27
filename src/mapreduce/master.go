package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Create three channels idle, jobInProgess and jobDone for workers
	jobInProgress := make(chan *DoJobArgs)
	jobDone := make(chan int)
	idle := make(chan string)

	getWorker := func() string {
		var address string
		select {
		case address = <- mr.registerChannel:
			mr.Workers[address] = &WorkerInfo{address}
		case address = <- idle:
		}
		return address
	}

	doTheJob := func(worker string, job *DoJobArgs) {
		var response DoJobReply
		ok := call(worker, "Worker.DoJob", job, &response)
		if ok {
			jobDone <- 1
			idle <- worker
		} else {
			fmt.Printf("error %s\n", worker)
			jobInProgress <- job
		}
	}

	// start go routines
	go func() {
		for job := range jobInProgress {
			worker := getWorker()
			go func(job *DoJobArgs) {
				doTheJob(worker, job)
			}(job)
		}
	}()

	// Go routine for Map Operation
	go func() {
		for i := 0; i < mr.nMap; i++ {
			job := &DoJobArgs{mr.file, Map, i, mr.nReduce}
			jobInProgress <- job
		}
	}()

	for i := 0; i < mr.nMap; i++ {
		<-jobDone
	}

	// Go routine for Reduce Operation
	go func() {
		for i := 0; i < mr.nReduce; i++ {
			job := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
			jobInProgress <- job
		}
	}()

	for i := 0; i < mr.nReduce; i++ {
		<-jobDone
	}

	// terminate jobInProgress channel
	close(jobInProgress)

	return mr.KillWorkers()
}
