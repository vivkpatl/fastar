package main

import (
	"io"
	"log"
	"mime/multipart"
)

// Helper struct to abstract away the complexity of single vs multi part
// range requests.
//
// Exposes simple Reset()/Read()/Close()/etc methods while transparently
// handling using single/multi part range requests to serve them.
type Reader struct {
	// Fields that don't change over lifecycle of reader
	Size, ChunkSize   int64
	NumWorkers        int
	SupportsMultipart bool
	Downloader        Downloader
	// Can change over reader lifecycle
	Start, CurPos   int64
	Chunk           io.ReadCloser
	MultipartReader *multipart.Reader
	MultipartChunk  *multipart.Part
}

func NewReader(size, start, chunkSize int64, numWorkers int, supportsMultipart bool, downloader Downloader) *Reader {
	r := &Reader{
		Start:             start,
		CurPos:            start,
		Size:              size,
		ChunkSize:         chunkSize,
		NumWorkers:        numWorkers,
		SupportsMultipart: supportsMultipart,
		Downloader:        downloader,
	}
	r.Reset()
	return r
}

// Reset all volatile state to start over reading contents from the current position.
// Useful if we abort a chunk midway and need to restart our progress from that chunk.
func (r *Reader) Reset() {
	r.Start = r.CurPos
	if r.UseMultipart() {
		r.MultipartReader = getMultipartReader(r.Downloader, r.Start, r.ChunkSize, r.Size, r.NumWorkers)
	}
}

func (r *Reader) AdvanceNextChunk() {
	r.CurPos += (r.ChunkSize * int64(r.NumWorkers))
}

func (r *Reader) RequestNextChunk() {
	if r.UseMultipart() {
		if multiChunk, err := r.MultipartReader.NextPart(); err == nil {
			r.MultipartChunk = multiChunk
		} else {
			log.Fatal("Error getting next multipart chunk:", err.Error())
		}
	} else {
		r.Chunk = r.Downloader.GetRange(r.CurPos, min(r.CurPos+r.ChunkSize, r.Size))
	}
}

func (r *Reader) Read(d []byte) (int, error) {
	if r.UseMultipart() {
		return r.MultipartChunk.Read(d)
	} else {
		return r.Chunk.Read(d)
	}
}

func (r *Reader) Close() error {
	if r.UseMultipart() {
		return r.MultipartChunk.Close()
	} else {
		return r.Chunk.Close()
	}
}

// Don't use multipart if the current request is too small to need it.
// Otherwise the response won't contain multipart boundary tokens
// and the reader object will break.
func (r *Reader) UseMultipart() bool {
	return r.SupportsMultipart && r.Start+r.ChunkSize*int64(r.NumWorkers) < r.Size
}

func getMultipartReader(
	downloader Downloader,
	start int64,
	chunkSize int64,
	size int64,
	numWorkers int) *multipart.Reader {
	var ranges = [][]int64{}
	var curChunkStart = start
	for curChunkStart < size {
		ranges = append(ranges, []int64{curChunkStart, min(curChunkStart+chunkSize, size)})
		curChunkStart += (chunkSize * int64(numWorkers))
	}
	var reader, err = (downloader).GetRanges(ranges)
	if err != nil {
		log.Fatal("Failed to get ranges from file:", err.Error())
	}
	return reader
}

func min(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}
