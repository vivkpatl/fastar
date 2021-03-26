package main

import (
	"fmt"
	"io"
	"os"
	"strconv"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	url          = kingpin.Arg("url", "URL to download from").Required().String()
	numWorkers   = kingpin.Flag("download-workers", "How many parallel workers to download the file").Default("8").Int()
	chunkSize    = kingpin.Flag("chunk-size", "Size of file chunks (in MB) to pull in parallel").Default("100").Uint64()
	outputDir    = kingpin.Flag("directory", "Directory to extract tarball to. Dumps file to stdout if not specified.").Short('C').ExistingDir()
	writeWorkers = kingpin.Flag("write-workers", "How many parallel workers to use to write file to disk").Default("1").Int()
)

func main() {
	kingpin.Parse()
	*chunkSize = *chunkSize * 1000000
	size, fileStream := GetDownloadStream(*url, *chunkSize, *numWorkers)

	fmt.Fprintln(os.Stderr, "File Size: "+strconv.FormatUint(size, 10))
	fmt.Fprintln(os.Stderr, "Num Download Workers: "+strconv.Itoa(*numWorkers))
	fmt.Fprintln(os.Stderr, "Chunk Size: "+strconv.FormatUint(*chunkSize, 10))
	fmt.Fprintln(os.Stderr, "Num Disk Workers: "+strconv.Itoa(*writeWorkers))

	if *outputDir == "" {
		io.CopyN(os.Stdout, fileStream, int64(size))
	} else {
		ExtractTarGz(fileStream)
	}
}
