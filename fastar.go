package main

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/jessevdk/go-flags"
	"github.com/pierrec/lz4"
)

var opts struct {
	NumWorkers      int               `long:"download-workers" default:"4" description:"How many parallel workers to download the file"`
	ChunkSize       int64             `long:"chunk-size" default:"200" description:"Size of file chunks (in MB) to pull in parallel"`
	OutputDir       string            `long:"directory" short:"C" description:"Directory to extract tarball to. Defaults to current dir if not specified"`
	ToStdout        bool              `long:"to-stdout" short:"O" description:"Dump downloaded file to stdout rather than extracting to disk"`
	WriteWorkers    int               `long:"write-workers" default:"8" description:"How many parallel workers to use to write file to disk"`
	StripComponents int               `long:"strip-components" description:"Strip STRIP-COMPONENTS leading components from file names on extraction"`
	Compression     string            `long:"compression" choice:"tar" choice:"gzip" choice:"lz4" description:"Force specific compression schema instead of inferring from magic bytes or filename extension"`
	RetryCount      int               `long:"retry-count" default:"4" description:"Max number of retries for a single chunk (exponential backoff starting at --retry-wait seconds)"`
	RetryWait       int               `long:"retry-wait" default:"1" description:"Starting number of seconds to wait in between retries (2x every retry)"`
	MaxWait         int               `long:"max-wait" default:"10" description:"Exponential retry wait is capped at this many seconds"`
	MinSpeed        string            `long:"min-speed" default:"1K" description:"Minimum speed per each chunk download. Retries and then fails if any are slower than this. 0 for no min speed, append K or M for KBps or MBps"`
	MinSpeedWait    int               `long:"min-speed-wait" default:"5" description:"How long to wait in seconds for download to stabilize before enforcing min speed"`
	ConnTimeout     int               `long:"connection-timeout" default:"60" description:"Abort download if TCP dial takes longer than this many seconds. Only supported for S3 and HTTP schemes."`
	IgnoreNodeFiles bool              `long:"ignore-node-files" description:"Don't throw errors on character or block device nodes"`
	Overwrite       bool              `long:"overwrite" description:"Overwrite any existing files"`
	Headers         map[string]string `long:"headers" short:"H" description:"Headers to use with http request"`
	UseFips         bool              `long:"use-fips-endpoint" description:"Use FIPS endpoint when downloading from S3"`
	DisableHttp2    bool              `long:"disable-http2" description:"Disable http2 to avoid reusing connections for GCS downloads"`
	UseGetForSize   bool              `long:"use-get-for-size" description:"Use GET with Range header instead of HEAD to determine file size for HTTP(S) URLs. Assumes RANGE support on the server side."`
	ExitStatusCodes []int             `long:"exit-status-codes" description:"HTTP status codes that should cause fastar to exit early. Specify each code separately (e.g. --exit-status-codes=401 --exit-status-codes=403)"`
}

var minSpeedBytesPerMillisecond = 0.0

// Magic byte sequences prepended to the start of every gzip or lz4
// compressed bundle. When downloading a file we can check for either
// of these sequences to automatically infer if we need to perform
// decompression, as well as which compression schema was used.
const (
	gzipMagicNumber = "1f8b"
	lz4MagicNumber  = "04224d18"
)

type CompressionType int

const (
	Tar CompressionType = iota
	Gzip
	Lz4
)

// filterUnknownFlags separates unknown flags from clean arguments
func filterUnknownFlags(args []string) (cleanArgs []string, unknownFlags []string) {
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") {
			unknownFlags = append(unknownFlags, arg)
		} else {
			cleanArgs = append(cleanArgs, arg)
		}
	}
	return cleanArgs, unknownFlags
}

func main() {
	var parser = flags.NewParser(&opts, flags.HelpFlag|flags.IgnoreUnknown)
	args, err := parser.Parse()
	if err != nil {
		// Handle help requests - go-flags writes help automatically when there's an error
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			// Print the error (which contains the help message) and exit
			fmt.Fprintln(os.Stderr, err)
			os.Exit(0)
		}
		// With IgnoreUnknown, we shouldn't get unknown flag errors, but handle other parsing errors
		log.Fatal("Failed to parse arguments: ", err)
	}
	
	// Filter out likely unknown flags from positional arguments and warn about them
	cleanArgs, unknownFlags := filterUnknownFlags(args)
	
	if len(unknownFlags) > 0 {
		log.Printf("Warning: Unknown arguments will be ignored: %v", unknownFlags)
	}
	
	if len(cleanArgs) == 0 {
		log.Fatal("Please pass source URL to download file from")
	}
	
	args = cleanArgs
	var rawUrl = args[0]
	processMinSpeedFlag()
	opts.ChunkSize *= 1e6 // Convert chunk size from MB to B
	fileStream := GetDownloadStream(GetDownloader(rawUrl, opts.UseFips, opts.UseGetForSize), opts.ChunkSize, opts.NumWorkers)

	url, err := url.Parse(rawUrl)
	if err != nil {
		log.Fatal("Failed to parse url: ", err.Error())
	}
	filename := path.Base(url.Path)

	log.Println("File name: " + filename)
	log.Printf("Num Download Workers: %d", opts.NumWorkers)
	log.Printf("Chunk Size (Mib): %d", opts.ChunkSize/1e6)
	log.Printf("Num Disk Workers: %d", opts.WriteWorkers)

	magicNumber, splicedStream := getMagicNumber(fileStream)

	compressionType := getCompressionType(filename, magicNumber)

	var finalStream io.Reader
	if compressionType == Lz4 {
		finalStream = lz4.NewReader(splicedStream)
	} else if compressionType == Gzip {
		finalStream, err = gzip.NewReader(splicedStream)
		if err != nil {
			log.Fatal("Error creating gzip stream: ", err.Error())
		}
	} else if compressionType == Tar {
		finalStream = splicedStream
	} else {
		log.Fatal("CompressionType not set, should be impossible")
	}

	if opts.ToStdout {
		if _, err := io.Copy(os.Stdout, finalStream); err != nil {
			log.Fatal("Failed to write file to stdout: ", err.Error())
		}
	} else {
		if opts.OutputDir == "" {
			if opts.OutputDir, err = os.Getwd(); err != nil {
				log.Fatal("Failed to get current working directory: ", err.Error())
			}
		}
		ExtractTar(finalStream)
	}
}

// Reads first few bytes from file stream to get any possible
// magic numbers, returns a spliced-together reader since
// the original io.Reader has already been read from.
func getMagicNumber(reader io.Reader) (string, io.Reader) {
	buf := make([]byte, 4)
	totalRead := 0
	for totalRead < 4 {
		read, err := reader.Read(buf[totalRead:])
		if err != nil && err != io.EOF {
			log.Fatal("Failed to read magic number:", err.Error())
		}
		totalRead += read
		if err == io.EOF {
			return "", io.LimitReader(bytes.NewReader(buf[:totalRead]), int64(totalRead))
		}
	}
	magicNumber := hex.EncodeToString(buf)
	splicedStream := io.MultiReader(bytes.NewReader(buf), reader)
	return magicNumber, splicedStream
}

// Choose compression type by the following preference order:
// 1. User provided compression type flag
// 2. Inferred by magic number
// 3. Inferred by file extension prefix in URL
// 4. Default to raw tarball
func getCompressionType(filename string, magicNumber string) CompressionType {
	if opts.Compression != "" {
		if opts.Compression == "tar" {
			log.Println("Forcing raw tar")
			return Tar
		} else if opts.Compression == "gzip" {
			log.Println("Forcing gzip")
			return Gzip
		} else {
			log.Println("Forcing lz4")
			return Lz4
		}
	} else {
		if strings.HasPrefix(magicNumber, gzipMagicNumber) {
			log.Println("Inferring gzip by magic number")
			return Gzip
		} else if strings.HasPrefix(magicNumber, lz4MagicNumber) {
			log.Println("Inferring lz4 by magic number")
			return Lz4
		} else {
			log.Println("Unrecognized magic number, falling back to file extension")
			if strings.HasSuffix(filename, "lz4") {
				log.Println("Inferring lz4 by file extension")
				return Lz4
			} else if strings.HasSuffix(filename, "gz") {
				log.Println("Inferring gzip by file extension")
				return Gzip
			} else if strings.HasSuffix(filename, "tar") {
				log.Println("Inferring raw tar by file extension")
				return Tar
			} else {
				log.Println("Unrecognized file extension, assuming raw tar")
				return Tar
			}
		}
	}
}

func processMinSpeedFlag() {
	var bytesPerSecond int
	var err error
	if strings.HasSuffix(opts.MinSpeed, "K") {
		if bytesPerSecond, err = strconv.Atoi((opts.MinSpeed)[:len(opts.MinSpeed)-1]); err != nil {
			log.Fatal("Failed to parse min speed argument", opts.MinSpeed, err.Error())
		}
		bytesPerSecond *= 1e3
	} else if strings.HasSuffix(opts.MinSpeed, "M") {
		if bytesPerSecond, err = strconv.Atoi((opts.MinSpeed)[:len(opts.MinSpeed)-1]); err != nil {
			log.Fatal("Failed to parse min speed argument", opts.MinSpeed, err.Error())
		}
		bytesPerSecond *= 1e6
	} else {
		if bytesPerSecond, err = strconv.Atoi(opts.MinSpeed); err != nil {
			log.Fatal("Failed to parse min speed argument", opts.MinSpeed, err.Error())
		}
	}
	minSpeedBytesPerMillisecond = float64(bytesPerSecond) / 1e3
}
