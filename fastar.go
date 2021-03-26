package main

import (
	"fmt"
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
	ExtractTarGz(fileStream)
	fmt.Fprintln(os.Stderr, size)
	// ExtractTarGz(io.LimitReader(fileStream, int64(size)))
	// io.CopyN(os.Stdout, fileStream, int64(size))

	// wg.Wait()
}
