package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
type WorkersList struct {
	sync.Mutex
	workersArr []string
	freeWorkers []int
	workerAvailable *sync.Cond
	allDone *sync.Cond
}

func (workers *WorkersList) addWorkers(registerChan chan string) {
	for worker := range registerChan {
		workers.Lock()
		fmt.Println("took a lock for addworkers")
		workers.workersArr = append(workers.workersArr, worker)
		workers.freeWorkers = append(workers.freeWorkers, len(workers.workersArr)-1)
		fmt.Println("unlock for addworkers")
		workers.Unlock()
		workers.workerAvailable.Signal()
	}
}

func (workers *WorkersList) getFreeWorkerIdx() int {
	workers.Lock()
	fmt.Println("took a lock for getFreeWorkerIdx")
	//defer workers.Unlock()
	if len(workers.freeWorkers) == 0 {
		fmt.Println("unlock for getFreeWorkerIdx, returning -1")
		workers.Unlock()
		return -1
	}
	freeWorkerIdx := workers.freeWorkers[0]
	workers.freeWorkers = workers.freeWorkers[1:]
	fmt.Println("remvoed from workersList.freeWorkers, newsize: ", len(workers.freeWorkers))
	fmt.Println("unlock for getFreeWorkerIdx")
	workers.Unlock()
	return freeWorkerIdx
}

func do_call(srv string, rpcname string, args interface{}, reply interface{}, workersList *WorkersList, freeWorker int) {
	call(srv, rpcname, args, reply)
	workersList.Lock()
	fmt.Println("took a lock for do_call")
	workersList.freeWorkers = append(workersList.freeWorkers, freeWorker)
	fmt.Println("appended to workersList.freeWorkers, newsize: ", len(workersList.freeWorkers))
	fmt.Println("unlock for do_call")
	if len(workersList.freeWorkers) == len(workersList.workersArr) {
		workersList.allDone.Signal()
	}
	workersList.Unlock()
	workersList.workerAvailable.Signal()
}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	fmt.Println("in schedule")
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Println("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

    /*
    create a datastructure to track which task is with which worker (mapfile->worker(recieved from registerChan)
    when you get a worker from registerChan, assign it a task based on phase, update the datastructure, and wait for completion
    when all tasks are completed, return
    */
    //workerMap := make(map[string]chan)
    workersList := new(WorkersList)
    workersList.workerAvailable = sync.NewCond(workersList)
    workersList.allDone = sync.NewCond(workersList)
    go workersList.addWorkers(registerChan)
    fmt.Println("**1")
    donetasks := 0
    for donetasks < ntasks {
    	    fmt.Println("**2")
    	    fmt.Println("number of donetasks are: ", donetasks, " number of ntasks: ", ntasks)
    	    fmt.Println("number of workers: ", len(workersList.workersArr))
    	    fmt.Println("number of freeworkers: ", len(workersList.freeWorkers))
    	    freeWorker := workersList.getFreeWorkerIdx()
    	    if freeWorker == -1 {
    	    	    fmt.Println("**3")
    	    	    workersList.Lock()
    	    	    fmt.Println("took a lock for schedule")
    	    	    workersList.workerAvailable.Wait()
    	    	    fmt.Println("unlock for schedule")
    	    	    workersList.Unlock()
    	    	    continue
    	    }
    	    fmt.Println("**4")
    	    var taskArgs DoTaskArgs
        taskArgs.JobName = jobName
        taskArgs.File = mapFiles[donetasks]
        fmt.Println("**5")
        taskArgs.NumOtherPhase = n_other
        taskArgs.Phase = phase
        taskArgs.TaskNumber = donetasks
        donetasks++
        fmt.Println("**6")
        go do_call(workersList.workersArr[freeWorker], "Worker.DoTask", taskArgs, nil, workersList, freeWorker)
        fmt.Println("**7")
    }
    
    for len(workersList.freeWorkers) < len(workersList.workersArr) {
       	workersList.Lock()
    	    workersList.allDone.Wait()
    	    workersList.Unlock()
    }
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Println("Schedule: %v done\n", phase)
}
