package main

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"log"
	"os"
)

func ExtractTarGz() {
	gzipStream, err := os.Open("file.tar.gz")
	if err != nil {
		panic(err)
	}
	defer gzipStream.Close()

	uncompressedStream, err := gzip.NewReader(gzipStream)
	if err != nil {
		log.Fatal("ExtractTarGz: NewReader failed")
	}

	tarReader := tar.NewReader(uncompressedStream)

	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf("ExtractTarGz: Next() failed: %s", err.Error())
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.Mkdir(header.Name, 0755); err != nil {
				log.Fatalf("ExtractTarGz: Mkdir() failed: %s", err.Error())
			}
		case tar.TypeReg:
			writeFile(header.Name, tarReader)
		default:
			log.Fatal(
				"ExtractTarGz: uknown type:",
				header.Typeflag,
				"in",
				header.Name)
		}
	}
}

func writeFile(filename string, tarReader *tar.Reader) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("ExtractTarGz: Create() failed: %s", err.Error())
	}
	defer file.Close()
	if _, err := io.Copy(file, tarReader); err != nil {
		log.Fatalf("ExtractTarGz: Copy() failed: %s", err.Error())
	}
}
