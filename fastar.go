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
	size       = uint64(4294967296)
	numWorkers = 16
	chunkSize  = uint64(2 << 20)
)

func main() {
	fmt.Fprint(os.Stderr, "hello world\n")

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
			fmt.Fprintln(os.Stderr, "Finished "+strconv.Itoa(idx))
			// nextChan <- true
			return
		}
		req, err := http.NewRequest("GET", "http://localhost:8000/rand", nil)
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
