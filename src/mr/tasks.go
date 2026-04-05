package mr

import (
	"errors"

	"github.com/google/uuid"
	"github.com/samber/lo"
)

type TaskId string

type TaskStatus int

const (
	TaskStatusPending TaskStatus = iota
	TaskStatusInProgress
	TaskStatusCompleted
)

type TaskType int

const (
	TaskTypeMap TaskType = iota
	TaskTypeReduce
)

var (
	ErrTaskNotFound = errors.New("task not found")
)

type Task struct {
	Id     TaskId
	Type   TaskType
	Status TaskStatus

	// Map task data
	Chunk      FileChunk
	NPartition int

	// Reduce task data
	Partition int
	NMap      int
}

func NewMapTask(chunk FileChunk, nPartition int) Task {
	return Task{
		Id:         generateTaskId(),
		Type:       TaskTypeMap,
		Status:     TaskStatusPending,
		Chunk:      chunk,
		NPartition: nPartition,
	}
}

func NewReduceTask(partition int, nMap int) Task {
	return Task{
		Id:        generateTaskId(),
		Type:      TaskTypeReduce,
		Status:    TaskStatusPending,
		Partition: partition,
		NMap:      nMap,
	}
}

type TasksRepo interface {
	AddTask(task Task)
	UpdateTask(taskId TaskId, updateFn func(task *Task) error) error

	GetTaskById(id TaskId) *Task
	GetTasksByStatus(status TaskStatus) []*Task
	GetTasksByType(taskType TaskType) []*Task
	GetTasksByTypeAndStatusAndServer(taskType TaskType, status TaskStatus, server string) []*Task
	GetTasksByTypeAndStatus(taskType TaskType, status TaskStatus) []*Task
}

type TasksRepoImpl struct {
	tasks                         []Task
	tasksById                     map[TaskId]*Task
	tasksByStatus                 map[TaskStatus][]*Task
	tasksByType                   map[TaskType][]*Task
	tasksByTypeAndStatus          map[lo.Tuple2[TaskType, TaskStatus]][]*Task
	tasksByTypeAndStatusAndServer map[lo.Tuple3[TaskType, TaskStatus, string]][]*Task
}

func NewTasksRepo() *TasksRepoImpl {
	return &TasksRepoImpl{
		tasks:                         []Task{},
		tasksById:                     make(map[TaskId]*Task),
		tasksByStatus:                 make(map[TaskStatus][]*Task),
		tasksByType:                   make(map[TaskType][]*Task),
		tasksByTypeAndStatus:          make(map[lo.Tuple2[TaskType, TaskStatus]][]*Task),
		tasksByTypeAndStatusAndServer: make(map[lo.Tuple3[TaskType, TaskStatus, string]][]*Task),
	}
}

func (r *TasksRepoImpl) AddTask(task Task) {
	r.tasks = append(r.tasks, task)
	r.indexTask(&task)
}

func (r *TasksRepoImpl) UpdateTask(taskId TaskId, updateFn func(task *Task) error) error {
	task, exists := r.tasksById[taskId]
	if !exists {
		return ErrTaskNotFound
	}

	// Remove task from current indices
	r.unindexTask(task)

	// Update task
	if err := updateFn(task); err != nil {
		return err
	}

	// Re-add task to indices with updated values
	r.indexTask(task)

	return nil
}

func (r *TasksRepoImpl) indexTask(task *Task) {
	r.tasksById[task.Id] = task
	r.tasksByStatus[task.Status] = append(r.tasksByStatus[task.Status], task)
	r.tasksByType[task.Type] = append(r.tasksByType[task.Type], task)
	r.tasksByTypeAndStatus[lo.T2(task.Type, task.Status)] = append(r.tasksByTypeAndStatus[lo.T2(task.Type, task.Status)], task)

	if task.Type == TaskTypeMap {
		r.tasksByTypeAndStatusAndServer[lo.T3(task.Type, task.Status, task.Chunk.Server)] = append(r.tasksByTypeAndStatusAndServer[lo.T3(task.Type, task.Status, task.Chunk.Server)], task)
	}
}

func (r *TasksRepoImpl) unindexTask(task *Task) {
	delete(r.tasksById, task.Id)
	r.tasksByStatus[task.Status] = lo.Without(r.tasksByStatus[task.Status], task)
	r.tasksByType[task.Type] = lo.Without(r.tasksByType[task.Type], task)
	r.tasksByTypeAndStatus[lo.T2(task.Type, task.Status)] = lo.Without(r.tasksByTypeAndStatus[lo.T2(task.Type, task.Status)], task)

	if task.Type == TaskTypeMap {
		r.tasksByTypeAndStatusAndServer[lo.T3(task.Type, task.Status, task.Chunk.Server)] = lo.Without(r.tasksByTypeAndStatusAndServer[lo.T3(task.Type, task.Status, task.Chunk.Server)], task)
	}
}

func (r *TasksRepoImpl) GetTaskById(id TaskId) *Task {
	return r.tasksById[id]
}

func (r *TasksRepoImpl) GetTasksByStatus(status TaskStatus) []*Task {
	return r.tasksByStatus[status]
}

func (r *TasksRepoImpl) GetTasksByType(taskType TaskType) []*Task {
	return r.tasksByType[taskType]
}

func (r *TasksRepoImpl) GetTasksByTypeAndStatusAndServer(taskType TaskType, status TaskStatus, server string) []*Task {
	return r.tasksByTypeAndStatusAndServer[lo.T3(taskType, status, server)]
}

func (r *TasksRepoImpl) GetTasksByTypeAndStatus(taskType TaskType, status TaskStatus) []*Task {
	return r.tasksByTypeAndStatus[lo.T2(taskType, status)]
}

func generateTaskId() TaskId {
	return TaskId(uuid.New().String())
}
