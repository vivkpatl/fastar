package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
)

var (
	url        = "http://localhost:8000/data"
	size       = uint64(4294967296)
	numWorkers = 16
	chunkSize  = uint64(2 << 20)
)

func main() {
	url = os.Args[1]
	resp, _ := http.Head(url)
	size = uint64(resp.ContentLength)
	numWorkers, _ = strconv.Atoi(os.Args[2])
	chunkSize, _ = strconv.ParseUint(os.Args[3], 10, 64)
	fmt.Fprintln(os.Stderr, "File Size: "+strconv.FormatUint(size, 10))
	fmt.Fprintln(os.Stderr, "Num Workers: "+strconv.Itoa(numWorkers))
	fmt.Fprintln(os.Stderr, "Chunk Size: "+strconv.FormatUint(chunkSize, 10))
	var chans []chan bool
	for i := 0; i < numWorkers; i++ {
		chans = append(chans, make(chan bool))
	}

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go writePartial(&wg, uint64(i)*chunkSize, chunkSize, chans[i], chans[(i+1)%numWorkers], i)
	}
	chans[0] <- true
	wg.Wait()
}

func writePartial(wg *sync.WaitGroup, start uint64, chunkSize uint64, curChan chan bool, nextChan chan bool, idx int) {
	defer wg.Done()
	client := &http.Client{}
	for {
		if start >= size {
			return
		}
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Fatal("Failed creating request:", err.Error())
		}
		req.Header.Add("Range", "bytes="+strconv.FormatUint(start, 10)+"-"+strconv.FormatUint(start+chunkSize-1, 10))
		resp, err := client.Do(req)
		if err != nil {
			log.Fatal("Failed get request:", err.Error())
		}
		// Wait until previous worker finished before we start writing to stdout
		<-curChan
		_, err = io.Copy(os.Stdout, resp.Body)
		if err != nil {
			log.Fatal("io copy failed:", err.Error())
		}
		// Trigger next worker to start writing to stdout.
		// Only send token if next worker has more work to do,
		// otherwise they already exited and won't be waiting
		// for a token.
		if start+chunkSize < size {
			nextChan <- true
		}
		start += (chunkSize * uint64(numWorkers))
	}
}
