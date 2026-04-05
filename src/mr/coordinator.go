package mr

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	mu                           sync.Mutex
	cfg                          Config
	tasks                        TasksRepo
	nMap                         int
	nReduce                      int
	partitionToIntermediateFiles map[int][]string
	timers                       map[TaskId]context.CancelFunc
}

func init() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	slog.SetDefault(logger)
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var assignedTask *Task
	if t := c.pickMapTask(args.Server); t != nil {
		assignedTask = t
	} else if t := c.pickReduceTask(); t != nil {
		assignedTask = t
	} else if len(c.tasks.GetTasksByStatus(TaskStatusPending)) > 0 {
		return ErrAllTasksBlocked
	} else {
		return ErrNoMoreTasks
	}

	slog.Info(fmt.Sprintf("assigned task %s of type %d to worker %s on server %s", assignedTask.Id, assignedTask.Type, args.WorkerId, args.Server))

	err := c.tasks.UpdateTask(assignedTask.Id, func(task *Task) error {
		task.Status = TaskStatusInProgress
		return nil
	})
	if err != nil {
		return err
	}

	*reply = AssignTaskReply{Task: *assignedTask}
	c.timers[assignedTask.Id] = c.startTaskTimer(assignedTask.Id)
	return nil
}

func (c *Coordinator) pickMapTask(server string) *Task {
	if tasks := c.tasks.GetTasksByTypeAndStatusAndServer(TaskTypeMap, TaskStatusPending, server); len(tasks) > 0 {
		return tasks[0]
	}
	if tasks := c.tasks.GetTasksByTypeAndStatus(TaskTypeMap, TaskStatusPending); len(tasks) > 0 {
		return tasks[0]
	}
	return nil
}

func (c *Coordinator) pickReduceTask() *Task {
	mapTasks := c.tasks.GetTasksByTypeAndStatus(TaskTypeMap, TaskStatusCompleted)
	if len(mapTasks) < c.nMap {
		return nil
	}

	if tasks := c.tasks.GetTasksByTypeAndStatus(TaskTypeReduce, TaskStatusPending); len(tasks) > 0 {
		return tasks[0]
	}
	return nil
}

func (c *Coordinator) startTaskTimer(taskId TaskId) context.CancelFunc {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.TaskTimeout())
	go func() {
		defer cancel()
		<-ctx.Done()

		if ctx.Err() == context.DeadlineExceeded {
			c.mu.Lock()
			defer c.mu.Unlock()

			task := c.tasks.GetTaskById(taskId)
			if task == nil {
				slog.Info(fmt.Sprintf("failed to get task by id %s", taskId))
				return
			}

			if task.Status != TaskStatusInProgress {
				slog.Info(fmt.Sprintf("task %s is not in progress, skipping timeout handling", taskId))
				return
			}

			err := c.tasks.UpdateTask(taskId, func(task *Task) error {
				task.Status = TaskStatusPending
				return nil
			})
			if err != nil {
				slog.Info(fmt.Sprintf("failed to update task %s: %v", taskId, err))
				return
			}

			slog.Info(fmt.Sprintf("task %s timed out and marked as pending again", taskId))
		}
	}()
	return cancel
}

func (c *Coordinator) ReportTaskCompletion(args *ReportTaskCompletionArgs, reply *ReportTaskCompletionReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task := c.tasks.GetTaskById(args.TaskId)
	if task == nil {
		slog.Error(fmt.Sprintf("failed to get task by id %s", args.TaskId))
		return ErrTaskNotFound
	}

	if task.Status != TaskStatusInProgress {
		slog.Debug(fmt.Sprintf("task %s is not in progress, cannot report completion", args.TaskId))
		return ErrTaskNotInProgress
	}

	err := c.tasks.UpdateTask(args.TaskId, func(task *Task) error {
		task.Status = TaskStatusCompleted
		return nil
	})
	if err != nil {
		return err
	}

	// Cancel task timer
	if cancel, ok := c.timers[args.TaskId]; ok {
		cancel()
		delete(c.timers, args.TaskId)
	} else {
		slog.Error(fmt.Sprintf("no timer found for task %s", args.TaskId))
	}

	if args.IntermediateFilePathByPartition != nil {
		for partition, filePath := range args.IntermediateFilePathByPartition {
			c.partitionToIntermediateFiles[partition] = append(c.partitionToIntermediateFiles[partition], filePath)
		}
	}
	slog.Debug(fmt.Sprintf("task %d %s completed", task.Type, args.TaskId))
	return nil
}

func (c *Coordinator) GetIntermediateFiles(args *GetIntermediateFilesArgs, reply *GetIntermediateFilesReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.IntermediateFilePaths = c.partitionToIntermediateFiles[args.Partition]
	reply.IsDone = len(c.tasks.GetTasksByTypeAndStatus(TaskTypeMap, TaskStatusCompleted)) == c.nMap

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		slog.Error(fmt.Sprintf("listen error %s: %v", sockname, e))
		os.Exit(1)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If all map and reduce tasks have been completed, return true
	return len(c.tasks.GetTasksByStatus(TaskStatusCompleted)) == c.nMap+c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	slog.Debug(fmt.Sprintf("starting coordinator with input files %v and nReduce %d", files, nReduce))

	// TODO: Use dependency injection
	tasksRepo := NewTasksRepo()
	fs := SingleNodeFS{}
	cfg := DefaultConfig{}

	// Check input files sizes and parition into chunks if necessary
	var chunks []FileChunk
	for _, file := range files {
		fileChunks, err := splitFileToChunks(file, fs, cfg.MaxChunkSize())
		if err != nil {
			slog.Error(fmt.Sprintf("failed to partition input file %s into chunks: %v", file, err))
			os.Exit(1)
		}
		chunks = append(chunks, fileChunks...)
	}

	for _, chunk := range chunks {
		tasksRepo.AddTask(NewMapTask(chunk, nReduce))
	}

	for i := 0; i < nReduce; i++ {
		tasksRepo.AddTask(NewReduceTask(i, len(chunks)))
	}

	c := Coordinator{
		tasks:                        tasksRepo,
		cfg:                          cfg,
		nMap:                         len(chunks),
		nReduce:                      nReduce,
		partitionToIntermediateFiles: make(map[int][]string, nReduce),
		timers:                       make(map[TaskId]context.CancelFunc),
	}

	c.server()
	return &c
}
