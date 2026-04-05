package mr

import "errors"

var (
	ErrAllTasksBlocked   = errors.New("all tasks are currently blocked, try again later")
	ErrNoMoreTasks       = errors.New("no more tasks available")
	ErrTaskNotInProgress = errors.New("task not in progress")
)
