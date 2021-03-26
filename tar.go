package main

import (
	"archive/tar"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

func ExtractTarGz(stream io.Reader) {
	fmt.Fprintln(os.Stderr, "Begin targz")
	tarReader := tar.NewReader(stream)

	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf("ExtractTarGz: Next() failed: %s", err.Error())
		}
		if strings.Contains(header.Name, "..") {
			fmt.Fprintln(os.Stderr, header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if header.Name == "./" {
				continue
			}
			if err := os.MkdirAll(header.Name, header.FileInfo().Mode()); err != nil {
				fmt.Fprintln(os.Stderr, header.Name)
				log.Fatalf("ExtractTarGz: Mkdir() failed: %s", err.Error())
			}
		case tar.TypeReg:
			writeFile(header.Name, tarReader)
		case tar.TypeLink:
			os.Link(header.Name, header.Linkname)
		case tar.TypeSymlink:
			os.Symlink(header.Name, header.Linkname)
		default:
			log.Fatal(
				"ExtractTarGz: uknown type:",
				header.Typeflag,
				" in ",
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
