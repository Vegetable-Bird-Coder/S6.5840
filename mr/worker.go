package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func reportStatus(taskId int, taskType string, success bool, outputFileNames []string) bool {
	args := ReportStatusArgs{TaskId: taskId, TaskType: taskType, Success: success, OutputFileNames: outputFileNames}
	reply := ReportStatusReply{}
	// fmt.Printf("%v task %d success: %v outputfile: %v\n", taskType, taskId, success, outputFileNames)
	return call("Coordinator.ReportStatus", &args, &reply)
}

func mapTask(mapf func(string, string) []KeyValue, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		args := GetInputArgs{}
		reply := GetInputReply{}
		ok := call("Coordinator.GetMapInput", &args, &reply)
		if !ok {
			fmt.Printf("get map input call failed!\n")
			return
		}

		// use tempfile to avoid partial writting
		outputFiles := []*os.File{}
		tempFileCreated := true
		for i := 0; i < reply.OutputFileNum; i++ {
			file, err := os.CreateTemp(".", fmt.Sprintf("map-temp-%d-%d-*", reply.TaskId, i))
			if err != nil {
				fmt.Printf("map taskId %v failed to create tempfile\n", reply.TaskId)
				for _, f := range outputFiles {
					f.Close()
					os.Remove(f.Name())
				}
				tempFileCreated = false
				break
			}
			outputFiles = append(outputFiles, file)
		}
		if !tempFileCreated {
			reportStatus(reply.TaskId, "map", false, nil)
			continue
		}

		// perform map operation on each file
		intermediate := make([][]KeyValue, reply.OutputFileNum)
		for i := range intermediate {
			intermediate[i] = make([]KeyValue, 0)
		}
		intermediateCreated := true
		for _, filename := range reply.InputFileNames {
			file, err := os.Open(filename)
			if err != nil {
				fmt.Printf("map taskId %d cannot open %v", reply.TaskId, filename)
				intermediateCreated = false
				break
			}
			content, err := io.ReadAll(file)
			if err != nil {
				fmt.Printf("map taskId %d cannot read %v", reply.TaskId, filename)
				intermediateCreated = false
				break
			}
			file.Close()
			kva := mapf(filename, string(content))
			for _, kv := range kva {
				idx := ihash(kv.Key) % reply.OutputFileNum
				intermediate[idx] = append(intermediate[idx], kv)
			}
		}
		if !intermediateCreated {
			reportStatus(reply.TaskId, "map", false, nil)
			continue
		}

		// store intermediate data into tempfile
		for i := range intermediate {
			sort.Sort(ByKey(intermediate[i]))
		}
		for i := range intermediate {
			for _, kv := range intermediate[i] {
				fmt.Fprintf(outputFiles[i], "%v %v\n", kv.Key, kv.Value)
			}
		}

		// rename tempfile
		outputFilesCreated := true
		outputFileNames := []string{}
		for i, outputFile := range outputFiles {
			newFileName := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
			err := os.Rename(outputFile.Name(), newFileName)
			if err != nil {
				fmt.Printf("map taskId %d failed to rename\n", reply.TaskId)
				for _, file := range outputFiles {
					file.Close()
					os.Remove(file.Name())
				}
				outputFilesCreated = false
				break
			}
			outputFileNames = append(outputFileNames, newFileName)
		}
		if !outputFilesCreated {
			reportStatus(reply.TaskId, "map", false, nil)
			continue
		}

		reportStatus(reply.TaskId, "map", true, outputFileNames)
	}
}

func reduceTask(reducef func(string, []string) string, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		args := GetInputArgs{}
		reply := GetInputReply{}
		ok := call("Coordinator.GetReduceInput", &args, &reply)
		if !ok {
			fmt.Printf("get reduce input call failed!\n")
			return
		}

		// use tempfile to avoid partial writting
		outputFile, err := os.CreateTemp(".", fmt.Sprintf("reduce-temp-%d-*", reply.TaskId))
		if err != nil {
			fmt.Printf("reduce taskId %v failed to create tempfile\n", reply.TaskId)
			reportStatus(reply.TaskId, "reduce", false, nil)
			continue
		}

		// load all key-value pair
		intermediate := []KeyValue{}
		intermediateCreated := true
		for _, inputFile := range reply.InputFileNames {
			file, err := os.Open(inputFile)
			if err != nil {
				fmt.Printf("reduce taskId %d cannot open %v", reply.TaskId, inputFile)
				intermediateCreated = false
				break
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				parts := strings.Fields(line)
				intermediate = append(intermediate, KeyValue{Key: parts[0], Value: parts[1]})
			}

			if err := scanner.Err(); err != nil {
				fmt.Printf("reduce taskId %d cannot read %v", reply.TaskId, inputFile)
				intermediateCreated = false
				break
			}
		}
		if !intermediateCreated {
			reportStatus(reply.TaskId, "reduce", false, nil)
			continue
		}
		sort.Sort(ByKey(intermediate))

		// perform reduce operation on intermediate data
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		// rename tempfile
		err = os.Rename(outputFile.Name(), fmt.Sprintf("mr-out-%d", reply.TaskId))
		if err != nil {
			fmt.Printf("reduce taskId %d failed to rename\n", reply.TaskId)
			outputFile.Close()
			os.Remove(outputFile.Name())
			reportStatus(reply.TaskId, "reduce", false, nil)
			continue
		}

		if ok := reportStatus(reply.TaskId, "reduce", true, []string{outputFile.Name()}); ok {
			for _, inputFile := range reply.InputFileNames {
				os.Remove(inputFile)
			}
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	var wg sync.WaitGroup
	wg.Add(1)
	go mapTask(mapf, &wg)

	wg.Add(1)
	go reduceTask(reducef, &wg)

	wg.Wait()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
