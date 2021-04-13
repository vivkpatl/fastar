package s3

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func getBucketAndKey(url string) (string, string) {
	parts := strings.Split(strings.Replace(url, "s3://", "", 1), "/")
	bucket := parts[0]
	key := strings.Join(parts[1:], "/")
	fmt.Fprintln(os.Stderr, bucket)
	fmt.Fprintln(os.Stderr, key)
	return bucket, key
}

// TODO: dedup this logic with the same in http_downloader.go
func GetDownloadStream(url string, chunkSize uint64, numWorkers int) (uint64, io.Reader) {
	bucket, key := getBucketAndKey(url)
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	req, resp := svc.GetObjectRequest(params)
	err := req.Send()
	if err != nil {
		log.Fatal("AWS request failed: ", err.Error())
	}
	fmt.Fprintln(os.Stderr, resp)
	size := uint64(*resp.ContentLength)

	var chans []chan bool
	for i := 0; i < numWorkers; i++ {
		chans = append(chans, make(chan bool, 1))
	}

	reader, writer := io.Pipe()

	for i := 0; i < numWorkers; i++ {
		go writePartial(
			bucket,
			key,
			size,
			svc,
			uint64(i)*chunkSize,
			chunkSize,
			numWorkers,
			writer,
			chans[i],
			chans[(i+1)%numWorkers],
			i)
	}
	chans[0] <- true
	return size, reader
}

func writePartial(
	bucket string,
	key string,
	size uint64,
	svc *s3.S3,
	start uint64,
	chunkSize uint64,
	numWorkers int,
	writer *io.PipeWriter,
	curChan chan bool,
	nextChan chan bool,
	idx int) {

	buf := make([]byte, chunkSize)
	r := bytes.NewReader(buf)
	for {
		if start >= size {
			return
		}
		params := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Range:  aws.String("bytes=" + strconv.FormatUint(start, 10) + "-" + strconv.FormatUint(start+chunkSize-1, 10)),
		}
		req, resp := svc.GetObjectRequest(params)
		err := req.Send()
		if err != nil {
			log.Fatal("AWS request failed: ", err.Error())
		}
		defer resp.Body.Close()

		// Read data off the wire and into an in memory buffer.
		// If this is the first chunk of the file, no point in first
		// reading it to a buffer before writing to stdout, we already need
		// it *now*.
		// For all other chunks, read them into an in memory buffer to greedily
		// force the chunk to be read off the wire. Otherwise we'd still be
		// bottlenecked by resp.Body.Read() when copying to stdout.
		if start > 0 {
			totalRead := 0
			for totalRead < int(*resp.ContentLength) {
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
		if *resp.ContentLength == int64(chunkSize) {
			r.Reset(buf)
		} else {
			r.Reset(buf[:*resp.ContentLength])
		}

		// Wait until previous worker finished before we start writing to stdout
		<-curChan
		if start > 0 {
			_, err = io.Copy(writer, r)
		} else {
			_, err = io.Copy(writer, resp.Body)
		}
		if err != nil {
			log.Fatal("io copy failed:", err.Error())
		}
		// Trigger next worker to start writing to stdout.
		// Only send token if next worker has more work to do,
		// otherwise they already exited and won't be waiting
		// for a token.
		// If next worker doesn't have work, we're handline
		// the last chunk so close the writer.
		if start+chunkSize < size {
			nextChan <- true
		} else {
			writer.Close()
		}
		start += (chunkSize * uint64(numWorkers))
	}
}
