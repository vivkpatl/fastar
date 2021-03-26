package main

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
)

var openFiles = 1000
var openFileTokens = make(chan bool, openFiles)

func ExtractTarGz(stream io.Reader) {
	fmt.Fprintln(os.Stderr, "Begin targz")
	tarReader := tar.NewReader(stream)
	target := "/home/cdenny/tmp/fastar"
	for i := 0; i < openFiles; i++ {
		openFileTokens <- true
	}
	fmt.Fprintln(os.Stderr, "got here")

	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("ExtractTarGz: Next() failed: %s", err.Error())
		}

		path := filepath.Join(target, header.Name)
		info := header.FileInfo()

		switch header.Typeflag {
		case tar.TypeDir:
			if _, err = os.Stat(path); os.IsExist(err) {
				continue
			}
			if err := os.MkdirAll(path, info.Mode()); err != nil {
				fmt.Fprintln(os.Stderr, header.Name)
				log.Fatalf("ExtractTarGz: Mkdir() failed: %s", err.Error())
			}
		case tar.TypeReg:
			buf := make([]byte, info.Size())
			totalRead := 0
			for totalRead < int(info.Size()) {
				read, err := tarReader.Read(buf[totalRead:])
				if err != nil && err != io.EOF {
					log.Fatal("Failed to read from resp:", err.Error())
				}
				totalRead += read
			}
			<-openFileTokens
			go writeFileAsync(path, buf, info)
			// writeFile(path, tarReader, info)
		case tar.TypeLink:
			// origDir, _ := filepath.Split(path)
			newPath := filepath.Join(target, header.Linkname)
			if _, err = os.Stat(newPath); os.IsNotExist(err) {
				file, err := os.Create(newPath)
				if err != nil {
					log.Fatal(err)
				}
				defer file.Close()
			}
			if err = os.Link(newPath, path); err != nil {
				fmt.Fprintln(os.Stderr, header.Linkname)
				fmt.Fprintln(os.Stderr, path)
				log.Fatal("Failed to hardlink: ", err.Error())
			}
		case tar.TypeSymlink:
			if err = os.Symlink(header.Linkname, path); err != nil {
				log.Fatal("Failed to symlink: ", err.Error())
			}
		default:
			log.Fatal(
				"ExtractTarGz: uknown type:",
				header.Typeflag,
				" in ",
				header.Name)
		}
		os.Chtimes(path, info.ModTime(), info.ModTime())
	}
}

func writeFile(filename string, tarReader *tar.Reader, info fs.FileInfo) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
	if err != nil {
		log.Fatal("Create file failed: ", err.Error())
	}
	defer file.Close()
	_, err = io.Copy(file, tarReader)
	if err != nil {
		log.Fatal("Copy file failed: ", err.Error())
	}
}

func writeFileAsync(filename string, buf []byte, info fs.FileInfo) {
	// fmt.Fprintln(os.Stderr, "opening new file")
	defer func() { openFileTokens <- true }()
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
	if err != nil {
		log.Fatal("Create file failed: ", err.Error())
	}
	defer file.Close()
	_, err = io.Copy(file, bytes.NewReader(buf))
	if err != nil {
		log.Fatal("Copy file failed: ", err.Error())
	}
}
