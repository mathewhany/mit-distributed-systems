package mr

import (
	"io"
	"os"
)

type FileChunk struct {
	Path   string
	Offset int64
	Size   int64
	Server string
}

type FileInfo struct {
	Name string
	Size int64
}

type Filesystem interface {
	Chunks(filePath string) ([]FileChunk, error)
	ReadFile(filePath string) (io.ReadSeekCloser, error)
	Fetch(path string, dest string) error
}

// SingleNodeFS is a simple implementation of the Filesystem interface for a
// single node filesystem. It treats each file as a single chunk.
type SingleNodeFS struct{}

func (s SingleNodeFS) Chunks(filePath string) ([]FileChunk, error) {
	// For a single node filesystem, a file chunk is just the whole file
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	return []FileChunk{{Path: filePath, Offset: 0, Size: fileInfo.Size()}}, nil
}

func (s SingleNodeFS) ReadFile(filePath string) (io.ReadSeekCloser, error) {
	return os.Open(filePath)
}

func (s SingleNodeFS) Fetch(path string, dest string) error {
	// Local filesystem doesn't need to fetch files, just copy them if needed
	if path != dest {
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()

		output, err := os.Create(dest)
		if err != nil {
			return err
		}
		defer output.Close()

		_, err = io.Copy(output, input)
		return err
	}
	return nil
}

// splitFileToChunks takes a file path and splits it into chunks of size at most
// MaxChunkSize, returning a list of FileChunk structs representing the chunks.
// It uses the underlying filesystem to determine the original file chunks, but
// may further split them if they are larger than MaxChunkSize.
func splitFileToChunks(filePath string, fs Filesystem, maxChunkSize int64) ([]FileChunk, error) {
	// Original file chunks are determined by the underlying filesystem, but we
	// may need to further split them into smaller chunks if they are larger
	// than maxChunkSize
	originalChunks, err := fs.Chunks(filePath)
	if err != nil {
		return nil, err
	}

	var returnedChunks []FileChunk
	for _, c := range originalChunks {
		returnedChunks = append(returnedChunks, splitChunk(c, maxChunkSize)...)
	}
	return returnedChunks, nil
}

// splitChunk takes a FileChunk and splits it into smaller chunks of size at
// most maxChunkSize, returning a list of FileChunk structs representing the
// smaller chunks. It is useful for splitting large file chunks into smaller
// chunks that can be processed by map tasks.
func splitChunk(chunk FileChunk, maxChunkSize int64) []FileChunk {
	var chunks []FileChunk
	var offset int64 = 0
	for offset < chunk.Size {
		chunkSize := maxChunkSize
		if offset+chunkSize > chunk.Size {
			chunkSize = chunk.Size - offset
		}
		chunks = append(chunks, FileChunk{Path: chunk.Path, Offset: chunk.Offset + offset, Size: chunkSize})
		offset += chunkSize
	}
	return chunks
}
