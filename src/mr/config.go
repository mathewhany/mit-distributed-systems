package mr

import "time"

type Config interface {
	MaxChunkSize() int64
	TaskTimeout() time.Duration
}

type DefaultConfig struct{}

func (c DefaultConfig) MaxChunkSize() int64 {
	return int64(10 * 1024 * 1024)
}

func (c DefaultConfig) TaskTimeout() time.Duration {
	return 10 * time.Second
}
