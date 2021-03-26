package main

import (
	"bytes"
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
	resp, err := http.Head(url)
	if err != nil {
		log.Fatal("Failed HEAD request for file size:", err.Error())
	}
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
	buf := make([]byte, chunkSize)
	r := bytes.NewReader(buf)
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

		// Read data off the wire and into an in memory buffer.
		// If this is the first chunk of the file, no point in first
		// reading it to a buffer before writing to stdout, we already need
		// it *now*.
		// For all other chunks, read them into an in memory buffer to greedily
		// force the chunk to be read off the wire. Otherwise we'd still be
		// bottlenecked by resp.Body.Read() when copying to stdout.
		if start > 0 {
			totalRead := 0
			for totalRead < int(resp.ContentLength) {
				read, err := resp.Body.Read(buf[totalRead:])
				if err != nil && err != io.EOF {
					log.Fatal("Failed to read from resp:", err.Error())
				}
				totalRead += read
			}
		}
		// Only slice the buffer for the case of the leftover data.
		// I saw a marginal slowdown when always slicing (even if
		// the slice was of the whole buffer)
		if resp.ContentLength == int64(chunkSize) {
			r.Reset(buf)
		} else {
			r.Reset(buf[:resp.ContentLength])
		}

		// Wait until previous worker finished before we start writing to stdout
		<-curChan
		if start > 0 {
			_, err = io.Copy(os.Stdout, r)
		} else {
			_, err = io.Copy(os.Stdout, resp.Body)
		}
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
