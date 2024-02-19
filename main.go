package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const Etag = "be04a3eb37076d6f0c479decb4e3738e-8"

func main() {
	if err := validateArgs(); err != nil {
		log.Fatalf("invalid args: %s", err)
	}

	url := "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-05.parquet"

	headResp, err := http.Head(url)
	if err != nil {
		log.Fatalf("error making HEAD request: %s", err)
	}
	defer headResp.Body.Close()

	contentLength := headResp.Header.Get("Content-Length")
	etag := strings.Replace(headResp.Header.Get("ETag"), "\"", "", -1)

	if etag != Etag {
		log.Fatalf("Wrong ETag value: %s, expected %s", etag, Etag)
	}

	log.Printf("File contentLength: %s bytes\n", contentLength)
	log.Printf("ETag: %s", etag)

	destinationFileName := "copy.parquet"
	err = os.Remove(destinationFileName)
	if err != nil {
		log.Fatalf("error removing file: %s", err)
	}

	if isSequential() {
		log.Println("Download file by sequential downloader")
		sequentialDownload(destinationFileName, url)
	} else {
		n := 8
		log.Println("Download file by parallel downloader")
		err = parallelDownload(destinationFileName, url, n)
		if err != nil {
			log.Fatalf("could not do parallel download: %s", err)
		}
	}

	// Confirm downloaded file matches md5 signature
	err = checkSignature(destinationFileName, "9338036cc8e8a2a002c2b2c036bdd0d4")
	if err != nil {
		log.Fatalf("error checking for signature: %s", err)
	}

	log.Println("All good!")
}

func validateArgs() error {
	if len(os.Args) < 2 {
		return errors.New("-s or -p are required. go run main.go -s|-p")
	}
	if !slices.Contains([]string{"-s", "-p"}, os.Args[1]) {
		return errors.New("only -s and -p are allowed. -s for sequential, -p for parallel")
	}
	return nil
}

func isSequential() bool {
	if len(os.Args) < 2 {
		return false
	}
	return os.Args[1] == "-s"
}

// sequentialDownload will download from url to destinationFileName in one go,
// without any concurrency involved
func sequentialDownload(destinationFileName, url string) {
	start := time.Now()
	getResp, err := http.Get(url)
	if err != nil {
		log.Fatalf("error making GET request: %s", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		log.Fatalf("wrong status received. expected %d, got %d", http.StatusOK, getResp.StatusCode)
	}

	f, err := os.Create(destinationFileName)
	if err != nil {
		log.Fatalf("could not open file for writing: %s", err)
	}
	defer f.Close()

	data, err := io.ReadAll(getResp.Body)
	if err != nil {
		log.Fatalf("error reading bytes from response: %s", err)
	}

	if _, err := f.Write(data); err != nil {
		log.Fatalf("could not write to file: %s", err)
	}
	log.Printf("All bytes downloaded & copied in: %f seconds\n", time.Since(start).Seconds())
}

// todo
func parallelDownload(destinationFileName, url string, n int) error {
	start := time.Now()

	headResp, err := http.Head(url)
	if err != nil {
		log.Fatalf("error making HEAD request: %s", err)
	}
	defer headResp.Body.Close()

	contentLength := headResp.Header.Get("Content-Length")
	size, err := strconv.Atoi(contentLength)
	if err != nil {
		return fmt.Errorf("error converting to int: %s", err)
	}

	err = createEmptyFile(destinationFileName, int64(size))
	if err != nil {
		return fmt.Errorf("error creating empty file: %s", err)
	}

	chunkSize := int64(size / n)
	lastChunkSize := chunkSize + int64(size%n)

	/**
	Each goroutine will get the URL, destination file name, offset and size to download.
	It’ll use an HTTP range request to download a chunk and write it to the correct section of the file.
	*/
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		offset := int64(i) * chunkSize
		go func() {
			defer wg.Done()

			if i == n-1 {
				downloadWorker(url, destinationFileName, offset, lastChunkSize)
			} else {
				downloadWorker(url, destinationFileName, offset, chunkSize)
			}
		}()
	}

	wg.Wait()

	log.Printf("All bytes downloaded & copied in: %f seconds\n", time.Since(start).Seconds())
	return nil
}

func downloadWorker(url string, destinationFileName string, offset, size int64) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Fatalf("could not create get request: %s", err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+size-1))

	rangeResp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("could not do Range request: %s", err)
	}
	defer rangeResp.Body.Close()

	log.Println("Downloaded", rangeResp.Header.Get("Content-Length"))

	data, err := io.ReadAll(rangeResp.Body)
	if err != nil {
		log.Fatalf("could not read bytes from the range resp: %s", err)
	}

	f, err := os.OpenFile(destinationFileName, os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("could not open file for writing")
	}
	f.WriteAt(data, offset)
}

// createEmptyFile creates an empty file in given size
func createEmptyFile(path string, size int64) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	file.Seek(size-1, os.SEEK_SET)
	file.Write([]byte{0})
	return nil
}

// checkSignature will check that file matches signature
func checkSignature(filename string, signature string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	actualSignature, err := calculateMD5(file)
	if err != nil {
		return err
	}

	if actualSignature != signature {
		log.Fatalf("md5sum of copy does not match the source file signature, expected %s, got %s", signature, actualSignature)
	}
	return nil
}

// calculateMD5 will calculate md5sum of the given file
func calculateMD5(file *os.File) (string, error) {
	hash := md5.New()

	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	hashInBytes := hash.Sum(nil)
	md5Signature := hex.EncodeToString(hashInBytes)

	return md5Signature, nil
}
