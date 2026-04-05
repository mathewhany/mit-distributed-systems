package mr

import (
	"fmt"
	"log/slog"
	"net/rpc"
	"os"
	"strconv"
)

type AssignTaskArgs struct {
	Server   string
	WorkerId string
}
type AssignTaskReply struct {
	Task
}

func AssignTask(args AssignTaskArgs) (AssignTaskReply, error) {
	slog.Debug(fmt.Sprintf("worker %s on server %s requesting task", workerId, args.Server))
	return call[AssignTaskReply]("Coordinator.AssignTask", args)
}

type ReportTaskCompletionArgs struct {
	TaskId TaskId

	// If task is a map task, the intermediate files produced by the map task
	IntermediateFilePathByPartition map[int]string
}
type ReportTaskCompletionReply struct{}

func ReportTaskCompletion(args ReportTaskCompletionArgs) (ReportTaskCompletionReply, error) {
	slog.Debug(fmt.Sprintf("worker %s on server %s reporting completion of task %s to coordinator with intermediate files %v", workerId, getServer(), args.TaskId, args.IntermediateFilePathByPartition))
	return call[ReportTaskCompletionReply]("Coordinator.ReportTaskCompletion", args)
}

type GetIntermediateFilesArgs struct {
	Partition int
}
type GetIntermediateFilesReply struct {
	IntermediateFilePaths []string
	IsDone                bool
}

func GetIntermediateFiles(args GetIntermediateFilesArgs) (GetIntermediateFilesReply, error) {
	slog.Debug(fmt.Sprintf("reduce task on worker %s requesting intermediate files for partition %d", workerId, args.Partition))
	return call[GetIntermediateFilesReply]("Coordinator.GetIntermediateFiles", args)
}

func call[Rep any, Args any](rpcname string, args Args) (Rep, error) {
	coordSockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		slog.Error("dialing error: %v", err)
		os.Exit(1)
	}
	defer c.Close()

	var reply Rep
	err = c.Call(rpcname, args, &reply)

	return reply, err
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
