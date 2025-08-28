package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandomString(n int64) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func TestGenerateRangeString(t *testing.T) {
	ranges := [][]int64{{0, 1}}
	rangeString := GenerateRangeString(ranges)
	expected := "bytes=0-0"
	if rangeString != expected {
		t.Fatalf("Got %s, wanted %s", rangeString, expected)
	}
	ranges = [][]int64{{0, 1}, {3, 6}}
	rangeString = GenerateRangeString(ranges)
	expected = "bytes=0-0,3-5"
	if rangeString != expected {
		t.Fatalf("Got %s, wanted %s", rangeString, expected)
	}
}

type TestDownloader struct {
	Data             string
	RangeSupport     bool
	MultipartSupport bool
}

func (testDownloader TestDownloader) GetFileInfo() (int64, bool, bool) {
	return int64(len(testDownloader.Data)), testDownloader.RangeSupport, testDownloader.MultipartSupport
}

func (testDownloader TestDownloader) Get() io.ReadCloser {
	return io.NopCloser(strings.NewReader(testDownloader.Data))
}

func (testDownloader TestDownloader) GetRange(start, end int64) io.ReadCloser {
	return io.NopCloser(strings.NewReader(testDownloader.Data[start:end]))
}

func (testDownloader TestDownloader) GetRanges(ranges [][]int64) (*multipart.Reader, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	for _, r := range ranges {
		part, err := writer.CreatePart(textproto.MIMEHeader{})
		if err != nil {
			log.Fatal(err)
		}
		_, err = io.Copy(part, strings.NewReader(testDownloader.Data[r[0]:r[1]]))
		if err != nil {
			log.Fatal(err)
		}
	}
	err := writer.Close()
	if err != nil {
		log.Fatal(err)
	}
	return multipart.NewReader(body, writer.Boundary()), nil
	// return nil, nil
}

func TestSingleReader(t *testing.T) {
	opts.RetryCount = 3
	for fileSize := int64(0); fileSize < 8; fileSize++ {
		data := RandomString(fileSize)
		downloader := TestDownloader{data, false, false}
		for chunkSize := int64(1); chunkSize < 4; chunkSize++ {
			for numWorkers := 1; numWorkers < 4; numWorkers++ {
				if bytes, err := io.ReadAll(GetDownloadStream(downloader, chunkSize, numWorkers)); err == nil {
					actual := string(bytes)
					if actual != data {
						t.Fatalf("Failed with fileSize: %d, chunkSize: %d, numWorkers: %d", fileSize, chunkSize, numWorkers)
					}
				} else {
					t.Fatalf("Unexpected error %v", err)
				}
			}
		}
	}
}

func TestRangeReader(t *testing.T) {
	opts.RetryCount = 3
	for fileSize := int64(0); fileSize < 8; fileSize++ {
		data := RandomString(fileSize)
		downloader := TestDownloader{data, true, false}
		for chunkSize := int64(1); chunkSize < 4; chunkSize++ {
			for numWorkers := 1; numWorkers < 4; numWorkers++ {
				if bytes, err := io.ReadAll(GetDownloadStream(downloader, chunkSize, numWorkers)); err == nil {
					actual := string(bytes)
					if actual != data {
						t.Fatalf("Failed with fileSize: %d, chunkSize: %d, numWorkers: %d", fileSize, chunkSize, numWorkers)
					}
				} else {
					t.Fatalf("Unexpected error %v", err)
				}
			}
		}
	}
}

func TestMultipartRangeReader(t *testing.T) {
	opts.RetryCount = 3
	for fileSize := int64(0); fileSize < 8; fileSize++ {
		data := RandomString(fileSize)
		downloader := TestDownloader{data, true, true}
		for chunkSize := int64(1); chunkSize < 4; chunkSize++ {
			for numWorkers := 1; numWorkers < 4; numWorkers++ {
				if bytes, err := io.ReadAll(GetDownloadStream(downloader, chunkSize, numWorkers)); err == nil {
					actual := string(bytes)
					if actual != data {
						t.Fatalf("Failed with fileSize: %d, chunkSize: %d, numWorkers: %d", fileSize, chunkSize, numWorkers)
					}
				} else {
					t.Fatalf("Unexpected error %v", err)
				}
			}
		}
	}
}

func TestHttpGetForSize(t *testing.T) {
	// Backup original options
	oldRetryCount := opts.RetryCount
	oldChunkSize := opts.ChunkSize
	
	// Set test options
	opts.RetryCount = 3
	opts.ChunkSize = 32 // Small chunks to trigger range support
	
	defer func() {
		// Restore original options
		opts.RetryCount = oldRetryCount
		opts.ChunkSize = oldChunkSize
	}()
	
	// Create a test server that serves a file
	testData := "Hello, World! This is test data for HTTP GET with Range header test."
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.Header.Get("Range") == "bytes=0-0" {
			// Respond to Range request with proper Content-Range header
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(testData)))
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusPartialContent)
			w.Write([]byte(testData[:1])) // Only return the first byte
		} else if r.Method == "HEAD" {
			// Traditional HEAD request
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusOK)
		} else if r.Method == "GET" && r.Header.Get("Range") == "" {
			// Full GET request
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(testData))
		}
	}))
	defer server.Close()

	// Test with useGetForSize = true
	downloader := HttpDownloader{
		Url:           server.URL,
		client:        server.Client(),
		useGetForSize: true,
	}
	
	size, supportsRange, supportsMultipart := downloader.GetFileInfo()
	
	if size != int64(len(testData)) {
		t.Errorf("Expected size %d, got %d", len(testData), size)
	}
	
	if !supportsRange {
		t.Error("Expected range support to be true")
	}
	
	if supportsMultipart {
		t.Error("Expected multipart support to be false")
	}
	
	fmt.Printf("HTTP GET with Range header test passed! Size: %d, Range: %v, Multipart: %v\n", 
		size, supportsRange, supportsMultipart)
}
