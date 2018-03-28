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

type Task struct {
	sync.Mutex
	remainingTasks []int
	scheduledTasks map[int]int //workerid to taskid map
	taskDone *sync.Cond
	ntasks int
}

func (tasks *Task) init(ntasks int) {
	tasks.Lock()
	defer tasks.Unlock()
	tasks.ntasks = ntasks
    tasks.remainingTasks = make([]int, 0)
    tasks.scheduledTasks = make(map[int]int)
    tasks.taskDone = sync.NewCond(tasks)
    for i := 0; i < ntasks; i++ {
    	    tasks.remainingTasks = append(tasks.remainingTasks, i)
    }
}

func (tasks *Task) getNextFreeTask() int {
	tasks.Lock()
	defer tasks.Unlock()
	if len(tasks.remainingTasks) == 0 {
		return -1
	}
	freeTask := tasks.remainingTasks[0]
	//tasks.remainingTasks = tasks.remainingTasks[1:]
	return freeTask
}

func (tasks *Task) markAsScheduled(taskId int, workerId int) {
	tasks.Lock()
	defer tasks.Unlock()
	t := make([]int, 0)
	for _,remainingTask := range(tasks.remainingTasks) {
		if remainingTask != taskId {
			t = append(t, remainingTask)
		}
	}
	tasks.remainingTasks = t
	tasks.scheduledTasks[workerId] = taskId
}

func (tasks *Task) markTaskAsDone(taskId int, workerId int) {
	tasks.Lock()
	defer tasks.Unlock()
	delete(tasks.scheduledTasks, workerId)
	tasks.taskDone.Signal()
}

func (tasks *Task) allTasksDone() bool {
	tasks.Lock()
	defer tasks.Unlock()
	if len(tasks.remainingTasks) == 0 && len(tasks.scheduledTasks) == 0 {
		return true
	}
	return false
}

func (tasks *Task) markTaskAsFailed(taskId int, workerId int) {
	tasks.Lock()
	defer tasks.Unlock()
	delete(tasks.scheduledTasks, workerId)
	tasks.remainingTasks = append(tasks.remainingTasks, taskId)
}

type WorkersList struct {
	sync.Mutex
	workersMap map[int]string //workerid to workerrpc
	freeWorkers []int
	numWorkers int
	workerAvailable *sync.Cond
}

func (workers *WorkersList) init() {
	workers.workersMap = make(map[int]string)
	workers.freeWorkers = make([]int, 0)
	workers.numWorkers = 0
	workers.workerAvailable = sync.NewCond(workers)
}

func (workers *WorkersList) addWorkers(registerChan chan string) {
	for worker := range registerChan {
		workers.Lock()
		workers.workersMap[workers.numWorkers] = worker
		workers.freeWorkers = append(workers.freeWorkers, workers.numWorkers)
		workers.numWorkers++
		workers.Unlock()
		workers.workerAvailable.Signal()
	}
}

func (workers *WorkersList) getFreeWorkerIdx() int {
	workers.Lock()
	//defer workers.Unlock()
	if len(workers.freeWorkers) == 0 {
		workers.Unlock()
		return -1
	}
	freeWorkerIdx := workers.freeWorkers[0]
	workers.freeWorkers = workers.freeWorkers[1:]
	workers.Unlock()
	return freeWorkerIdx
}

func (workers *WorkersList) markerWorkerAsFailed(workerId int) {
	workers.Lock()
	defer workers.Unlock()
	delete(workers.workersMap, workerId)
}

func do_call(srv string, rpcname string, args interface{}, reply interface{},
	workersList *WorkersList, freeWorker int, tasks *Task, taskId int) {
	if call(srv, rpcname, args, reply) {
		workersList.Lock()
		workersList.freeWorkers = append(workersList.freeWorkers, freeWorker)
		workersList.Unlock()
		workersList.workerAvailable.Signal()
		tasks.markTaskAsDone(taskId, freeWorker)
	} else {
		workersList.markerWorkerAsFailed(freeWorker)
		tasks.markTaskAsFailed(taskId, freeWorker)
	}
}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
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
    
    tasks := new(Task)
    tasks.init(ntasks)
    
    workersList := new(WorkersList)
    workersList.init()
    go workersList.addWorkers(registerChan)
    
    for !tasks.allTasksDone() {
    	    taskId := tasks.getNextFreeTask()
    	    if taskId == -1 {
    	    	    tasks.Lock()
    	    	    tasks.taskDone.Wait()
    	    	    tasks.Unlock()
    	    	    continue
    	    }
    	    workerId := workersList.getFreeWorkerIdx()
    	    if workerId == -1 {
    	    	    workersList.Lock()
    	    	    workersList.workerAvailable.Wait()
    	    	    workersList.Unlock()
    	    	    continue
    	    }
    	    var taskArgs DoTaskArgs
        taskArgs.JobName = jobName
        taskArgs.File = mapFiles[taskId]
        taskArgs.NumOtherPhase = n_other
        taskArgs.Phase = phase
        taskArgs.TaskNumber = taskId
        tasks.markAsScheduled(taskId, workerId)
    	    go do_call(workersList.workersMap[workerId], "Worker.DoTask", taskArgs, nil, workersList, workerId, tasks, taskId)
    }
 
	fmt.Println("Schedule: %v done\n", phase)
}
