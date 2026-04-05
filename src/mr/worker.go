package mr

import (
	"bufio"
	"cmp"
	"container/heap"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueWithBuffer struct {
	Key    string
	Value  string
	Buffer *bufio.Scanner
}

// Implement Heap
type KeyValueHeap []KeyValueWithBuffer

func (h KeyValueHeap) Len() int           { return len(h) }
func (h KeyValueHeap) Less(i, j int) bool { return cmp.Compare(h[i].Key, h[j].Key) < 0 }
func (h KeyValueHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *KeyValueHeap) Push(x any) {
	*h = append(*h, x.(KeyValueWithBuffer))
}

func (h *KeyValueHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var workerId = uuid.New().String()

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	for {
		task := getTask()

		switch task.Type {
		case TaskTypeMap:
			handleMapTask(task, mapf)
		case TaskTypeReduce:
			handleReduceTask(task, reducef)
		default:
			slog.Error(fmt.Sprintf("unknown task type: %v", task.Type))
			os.Exit(1)
		}
	}
}

func handleMapTask(task Task, mapf func(string, string) []KeyValue) {
	fs := SingleNodeFS{} // TODO: Inject

	r, err := fs.ReadFile(task.Chunk.Path)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to read file chunk for map task %s: %v", task.Id, err))
		return
	}
	defer r.Close()

	r.Seek(task.Chunk.Offset, io.SeekStart)

	sc := bufio.NewScanner(r)

	var totalBytesRead int64
	if task.Chunk.Offset > 0 {
		if sc.Scan() {
			totalBytesRead += int64(len(sc.Bytes())) + 1
		} else if err := sc.Err(); err != nil {
			slog.Error(fmt.Sprintf("failed to read file chunk for map task %s: %v", task.Id, err))
			return
		}
	}

	mapOutput := make(map[int][]KeyValue)

	var chunkBuilder strings.Builder
	for totalBytesRead < task.Chunk.Size && sc.Scan() {
		totalBytesRead += int64(len(sc.Bytes())) + 1
		chunkBuilder.WriteString(sc.Text())
		chunkBuilder.WriteByte('\n')
	}

	chunk := chunkBuilder.String()
	for _, kv := range mapf(task.Chunk.Path, chunk) {
		partition := ihash(kv.Key) % task.NPartition
		mapOutput[partition] = append(mapOutput[partition], kv)
	}

	if err := sc.Err(); err != nil {
		slog.Error(fmt.Sprintf("failed to read file chunk for map task %s: %v", task.Id, err))
		return
	}

	intermediateFilePathByPartition := make(map[int]string)
	for partition, kvs := range mapOutput {
		slices.SortFunc(kvs, func(a, b KeyValue) int {
			return cmp.Compare(a.Key, b.Key)
		})

		intermediateFilePath := fmt.Sprintf("mr-mapper-%d-%s", partition, task.Id)
		intermediateFilePathByPartition[partition] = intermediateFilePath

		// Open for write
		f, err := os.Create(intermediateFilePath) // TODO: Replace with fs
		if err != nil {
			slog.Error(fmt.Sprintf("failed to create intermediate file for map task %s: %v", task.Id, err))
			return
		}
		defer f.Close()

		// Write intermediate key-value pairs to intermediate file
		w := bufio.NewWriter(f)
		for _, kv := range kvs {
			if _, err := fmt.Fprintf(w, "%s %s\n", kv.Key, kv.Value); err != nil {
				slog.Error(fmt.Sprintf("failed to write intermediate file for map task %s: %v", task.Id, err))
				return
			}
		}
		if err := w.Flush(); err != nil {
			slog.Error(fmt.Sprintf("failed to flush intermediate file for map task %s: %v", task.Id, err))
			return
		}
	}

	// Reply to coordinator that map task is completed
	ReportTaskCompletion(ReportTaskCompletionArgs{
		TaskId:                          task.Id,
		IntermediateFilePathByPartition: intermediateFilePathByPartition,
	})
}

func handleReduceTask(task Task, reducef func(string, []string) string) {
	var wg sync.WaitGroup // TODO: replace with a worker that handles errors
	fs := SingleNodeFS{}  // TODO: Inject

	seenFiles := make(map[string]struct{})
	files := []string{}

	for {
		// Ask coordinator for intermediate file paths for assigned reduce partition
		reply, err := GetIntermediateFiles(GetIntermediateFilesArgs{
			Partition: task.Partition,
		})
		if err != nil {
			slog.Error(fmt.Sprintf("failed to get intermediate files for reduce task %s: %v", task.Id, err))
			return
		}

		// Fetch intermediate key-value pairs from intermediate files
		for i, path := range reply.IntermediateFilePaths {
			localPath := fmt.Sprintf("mr-reducer-%s-%d", task.Id, i)
			if _, exists := seenFiles[localPath]; !exists {
				seenFiles[localPath] = struct{}{}
				files = append(files, localPath)

				wg.Add(1)
				go func() {
					err := fs.Fetch(path, localPath)
					if err != nil {
						slog.Error(fmt.Sprintf("failed to fetch intermediate file for reduce task %s: %v", task.Id, err))
						return
					}
					wg.Done()
				}()
			}
		}

		if reply.IsDone {
			break
		}

		time.Sleep(time.Second)
	}

	wg.Wait()

	// Buffer read intermediate key-value pairs from intermediate files
	buffers := make([]*bufio.Scanner, len(files))
	for i, file := range files {
		f, err := os.Open(file)
		if err != nil {
			slog.Error(fmt.Sprintf("failed to open intermediate file for reduce task %s: %v", task.Id, err))
			return
		}
		defer f.Close()

		buffers[i] = bufio.NewScanner(f)
	}

	// Merge sort intermediate key-value pairs by key
	sortedFilePath := fmt.Sprintf("mr-reducer-sorted-%s", task.Id)
	sortedInputFile, err := os.Create(sortedFilePath)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to create sorted intermediate file for reduce task %s: %v", task.Id, err))
		return
	}
	defer sortedInputFile.Close()

	sortedKvBuffer := bufio.NewWriter(sortedInputFile)

	h := make(KeyValueHeap, 0)
	heap.Init(&h)

	for _, buffer := range buffers {
		if buffer.Scan() {
			line := buffer.Text()
			var k, v string
			if _, err := fmt.Sscanf(line, "%s %s", &k, &v); err != nil {
				slog.Error(fmt.Sprintf("failed to parse intermediate file for reduce task %s: %v", task.Id, err))
				return
			}

			heap.Push(&h, KeyValueWithBuffer{Key: k, Value: v, Buffer: buffer})
		} else if err := buffer.Err(); err != nil {
			slog.Error(fmt.Sprintf("failed to read intermediate file for reduce task %s: %v", task.Id, err))
			return
		}
	}

	for h.Len() > 0 {
		kv := heap.Pop(&h).(KeyValueWithBuffer)
		if _, err := fmt.Fprintf(sortedKvBuffer, "%s %s\n", kv.Key, kv.Value); err != nil {
			slog.Error(fmt.Sprintf("failed to write sorted intermediate file for reduce task %s: %v", task.Id, err))
			return
		}

		if kv.Buffer.Scan() {
			line := kv.Buffer.Text()
			var k, v string
			if _, err := fmt.Sscanf(line, "%s %s", &k, &v); err != nil {
				slog.Error(fmt.Sprintf("failed to parse intermediate file for reduce task %s: %v", task.Id, err))
				return
			}

			heap.Push(&h, KeyValueWithBuffer{Key: k, Value: v, Buffer: kv.Buffer})
		} else if err := kv.Buffer.Err(); err != nil {
			slog.Error(fmt.Sprintf("failed to read intermediate file for reduce task %s: %v", task.Id, err))
			return
		}
	}

	if err := sortedKvBuffer.Flush(); err != nil {
		slog.Error(fmt.Sprintf("failed to flush sorted intermediate file for reduce task %s: %v", task.Id, err))
		return
	}

	// Call reducef on each key and list of values, buffer write output key-value pair to output file
	outputFilePath := fmt.Sprintf("mr-out-%s-tmp-%s", task.Id, uuid.New().String())
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to create output file for reduce task %s: %v", task.Id, err))
		return
	}
	defer outputFile.Close()

	outputBuffer := bufio.NewWriter(outputFile)

	sortedInputFile.Seek(0, io.SeekStart)
	sortedInputFileScanner := bufio.NewScanner(sortedInputFile)

	var currentKey string
	var currentValues []string

	for sortedInputFileScanner.Scan() {
		line := sortedInputFileScanner.Text()
		var k, v string
		if _, err := fmt.Sscanf(line, "%s %s", &k, &v); err != nil {
			slog.Error(fmt.Sprintf("failed to parse sorted intermediate file for reduce task %s: %v", task.Id, err))
			return
		}

		if k != currentKey && currentKey != "" {
			outputValue := reducef(currentKey, currentValues)
			if _, err := fmt.Fprintf(outputBuffer, "%s %s\n", currentKey, outputValue); err != nil {
				slog.Error(fmt.Sprintf("failed to write output file for reduce task %s: %v", task.Id, err))
				return
			}
			currentValues = nil
		}

		currentKey = k
		currentValues = append(currentValues, v)
	}

	if len(currentValues) > 0 {
		outputValue := reducef(currentKey, currentValues)
		if _, err := fmt.Fprintf(outputBuffer, "%s %s\n", currentKey, outputValue); err != nil {
			slog.Error(fmt.Sprintf("failed to write output file for reduce task %s: %v", task.Id, err))
			return
		}
	}

	if err := sortedInputFileScanner.Err(); err != nil {
		slog.Error(fmt.Sprintf("failed to read sorted intermediate file for reduce task %s: %v", task.Id, err))
		return
	}

	if err := outputBuffer.Flush(); err != nil {
		slog.Error(fmt.Sprintf("failed to flush output file for reduce task %s: %v", task.Id, err))
		return
	}

	// Once done atomicly rename output file to final output file name
	finalOutputFilePath := fmt.Sprintf("mr-out-%d", task.Partition)
	if err := os.Rename(outputFilePath, finalOutputFilePath); err != nil {
		slog.Error(fmt.Sprintf("failed to rename output file for reduce task %s: %v", task.Id, err))
		return
	}

	// Reply to coordinator that reduce task is completed
	ReportTaskCompletion(ReportTaskCompletionArgs{
		TaskId: task.Id,
	})
}

func getServer() string {
	return os.Getenv("WORKER_SERVER")
}

func getTask() Task {
	reply, err := AssignTask(AssignTaskArgs{
		Server:   getServer(), // TODO: Inject
		WorkerId: workerId,
	})

	if err == ErrNoMoreTasks {
		slog.Info(fmt.Sprintf("worker %s: no more tasks, exiting...", getServer()))
		os.Exit(0)
	}

	if err != nil {
		slog.Error(fmt.Sprintf("worker %s failed to get task: %v", getServer(), err))
		time.Sleep(time.Second)
		return getTask()
	}

	return reply.Task
}
