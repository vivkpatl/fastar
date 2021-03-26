package main

import (
	"io"
	"os"
	"strconv"
	"sync"
)

var (
	wg = sync.WaitGroup{}
)

func main() {
	url := os.Args[1]
	numWorkers, _ := strconv.Atoi(os.Args[2])
	chunkSize, _ := strconv.ParseUint(os.Args[3], 10, 64)

	size, fileStream := GetDownloadStream(url, chunkSize, numWorkers)
	ExtractTarGz(io.LimitReader(fileStream, int64(size)))
	// io.CopyN(os.Stdout, fileStream, int64(size))

	wg.Wait()
}
