package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTaskExpiredTime    map[int]time.Time
	reduceTaskExpiredTime map[int]time.Time

	mapInputFiles     []string
	reduceInputFiles  []string
	reduceOutputFiles []string

	reduceWorkerNum     int
	mapTasksNum         int
	reduceInputFileNums int

	mapTaskIds    []int
	reduceTaskIds []int

	mapMutex    sync.Mutex
	mapCond     *sync.Cond
	reduceMutex sync.Mutex
	reduceCond  *sync.Cond

	done chan bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (c *Coordinator) GetMapInput(args *GetInputArgs, reply *GetInputReply) error {
	c.mapMutex.Lock()
	for len(c.mapTaskIds) == 0 {
		c.mapCond.Wait()
	}
	defer c.mapMutex.Unlock()

	taskId := c.mapTaskIds[0]
	c.mapTaskIds = c.mapTaskIds[1:]

	reply.InputFileNames = c.mapInputFiles[taskId*c.mapTasksNum : min((taskId+1)*c.mapTasksNum, len(c.mapInputFiles))]
	reply.OutputFileNum = c.reduceWorkerNum
	reply.TaskId = taskId

	c.mapTaskExpiredTime[taskId] = time.Now().Add(10 * time.Second)

	return nil
}

func (c *Coordinator) GetReduceInput(args *GetInputArgs, reply *GetInputReply) error {
	c.reduceMutex.Lock()
	for len(c.reduceTaskIds) == 0 {
		c.reduceCond.Wait()
	}
	defer c.reduceMutex.Unlock()

	taskId := c.reduceTaskIds[0]
	c.reduceTaskIds = c.reduceTaskIds[1:]

	for i := taskId; i < len(c.reduceInputFiles); i += c.reduceWorkerNum {
		reply.InputFileNames = append(reply.InputFileNames, c.reduceInputFiles[i])
	}
	reply.TaskId = taskId

	c.reduceTaskExpiredTime[taskId] = time.Now().Add(10 * time.Second)
	return nil
}

func (c *Coordinator) ReportStatus(args *ReportStatusArgs, reply *ReportStatusReply) error {
	if args.TaskType == "map" {
		c.mapMutex.Lock()
		defer c.mapMutex.Unlock()
		defer c.mapCond.Broadcast()
		// skip the straggler worker
		if _, ok := c.mapTaskExpiredTime[args.TaskId]; !ok {
			return nil
		}
		delete(c.mapTaskExpiredTime, args.TaskId)
		if !args.Success {
			c.mapTaskIds = append(c.mapTaskIds, args.TaskId)
		} else {
			c.reduceInputFiles = append(c.reduceInputFiles, args.OutputFileNames...)
			if len(c.reduceInputFiles) == c.reduceInputFileNums {
				for i := 0; i < c.reduceWorkerNum; i++ {
					c.reduceTaskIds = append(c.reduceTaskIds, i)
				}
				c.reduceCond.Broadcast()
			}
		}
	} else {
		c.reduceMutex.Lock()
		defer c.reduceMutex.Unlock()
		defer c.reduceCond.Broadcast()
		if _, ok := c.reduceTaskExpiredTime[args.TaskId]; !ok {
			return nil
		}
		delete(c.reduceTaskExpiredTime, args.TaskId)
		if !args.Success {
			c.reduceTaskIds = append(c.reduceTaskIds, args.TaskId)
		} else {
			c.reduceOutputFiles = append(c.reduceOutputFiles, args.OutputFileNames...)
			if len(c.reduceOutputFiles) == c.reduceWorkerNum {
				close(c.done)
			}
		}
	}
	return nil
}

// check whether the task is timeout
func (c *Coordinator) checkTask() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			{
				c.mapMutex.Lock()
				for id, expiredTime := range c.mapTaskExpiredTime {
					if expiredTime.Before(time.Now()) {
						delete(c.mapTaskExpiredTime, id)
						c.mapTaskIds = append(c.mapTaskIds, id)
					}
				}
				c.mapMutex.Unlock()
				c.mapCond.Broadcast()
			}
			{
				c.reduceMutex.Lock()
				for id, expiredTime := range c.reduceTaskExpiredTime {
					if expiredTime.Before(time.Now()) {
						delete(c.reduceTaskExpiredTime, id)
						c.reduceTaskIds = append(c.reduceTaskIds, id)

					}
				}
				c.reduceMutex.Unlock()
				c.reduceCond.Broadcast()
			}
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.done = make(chan bool)
	c.mapTaskExpiredTime = make(map[int]time.Time)
	c.reduceTaskExpiredTime = make(map[int]time.Time)
	c.reduceWorkerNum = nReduce
	c.mapInputFiles = files
	c.mapTasksNum = 1
	c.mapCond = sync.NewCond(&c.mapMutex)
	c.reduceCond = sync.NewCond(&c.reduceMutex)
	for mapTaskId := 0; mapTaskId*c.mapTasksNum < len(files); mapTaskId++ {
		c.mapTaskIds = append(c.mapTaskIds, mapTaskId)
	}
	c.reduceInputFileNums = len(c.mapTaskIds) * c.reduceWorkerNum

	go c.checkTask()

	c.server()
	return &c
}
