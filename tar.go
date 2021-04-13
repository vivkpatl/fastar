package main

import (
	"archive/tar"
	"bytes"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
)

var openFileTokens chan bool

func ExtractTar(stream io.Reader) {
	openFileTokens = make(chan bool, *writeWorkers)
	tarReader := tar.NewReader(stream)
	for i := 0; i < *writeWorkers; i++ {
		openFileTokens <- true
	}
	var wg sync.WaitGroup

	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("ExtractTarGz: Next() failed: %s", err.Error())
		}

		path := filepath.Join(*outputDir, header.Name)
		info := header.FileInfo()

		switch header.Typeflag {
		case tar.TypeDir:
			if _, err = os.Stat(path); os.IsExist(err) {
				continue
			}
			if err := os.MkdirAll(path, info.Mode()); err != nil {
				log.Fatalf("ExtractTarGz: Mkdir() failed: %s", err.Error())
			}
			os.Chmod(path, info.Mode().Perm()&0755)
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
			wg.Add(1)
			go writeFileAsync(path, buf, info, &wg)
			// writeFile(path, tarReader, info)
		case tar.TypeLink:
			newPath := filepath.Join(*outputDir, header.Linkname)
			if _, err = os.Stat(newPath); os.IsNotExist(err) {
				file, err := os.Create(newPath)
				if err != nil {
					log.Fatal(err)
				}
				defer file.Close()
			}
			if err = os.Link(newPath, path); err != nil {
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
	}
	wg.Wait()
}

func writeFile(filename string, tarReader *tar.Reader, info fs.FileInfo) {
	// passing in info.Mode() here doesn't even create the file with Mode() bits???
	// call os.Chmod afterwards with the same bits to fix this =_=
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
	if err != nil {
		log.Fatal("Create file failed: ", err.Error())
	}
	defer os.Chmod(filename, info.Mode().Perm()&0755)
	defer file.Close()
	_, err = io.Copy(file, tarReader)
	if err != nil {
		log.Fatal("Copy file failed: ", err.Error())
	}
}

func writeFileAsync(filename string, buf []byte, info fs.FileInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	defer func() { openFileTokens <- true }()
	// passing in info.Mode() here doesn't even create the file with Mode() bits???
	// call os.Chmod afterwards with the same bits to fix this =_=
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
	if err != nil {
		log.Fatal("Create file failed: ", err.Error())
	}
	defer os.Chmod(filename, info.Mode().Perm()&0755)
	defer file.Close()
	_, err = io.Copy(file, bytes.NewReader(buf))
	if err != nil {
		log.Fatal("Copy file failed: ", err.Error())
	}
}
