package downloader

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"
)

type Downloader struct {
	isParallel          bool
	numWorkers          int
	destinationFileName string
	url                 string
}

func NewDownloader(isParallel bool, destinationFileName, url string, numWorkers int) Downloader {
	return Downloader{
		isParallel:          isParallel,
		numWorkers:          numWorkers,
		destinationFileName: destinationFileName,
		url:                 url,
	}
}

func (d Downloader) Download() error {
	start := time.Now()
	defer func() {
		log.Printf("downloaded in %f seconds", time.Since(start).Seconds())
	}()
	if d.isParallel {
		return d.parallelDownload(d.destinationFileName, d.url, d.numWorkers)
	}
	return d.sequentialDownload(d.destinationFileName, d.url)
}

// sequentialDownload is doing 1 GET request and downloads all the data in the destinationFileName
func (d Downloader) sequentialDownload(destinationFileName, url string) error {
	getResp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error making GET request: %s", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		return fmt.Errorf("wrong status received. expected %d, got %d", http.StatusOK, getResp.StatusCode)
	}

	f, err := os.Create(destinationFileName)
	if err != nil {
		return fmt.Errorf("could not open file for writing: %s", err)
	}
	defer f.Close()

	data, err := io.ReadAll(getResp.Body)
	if err != nil {
		return fmt.Errorf("error reading bytes from response: %s", err)
	}

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("could not write to file: %s", err)
	}
	return nil
}

// parallelDownload will spin n goroutines that will download the file in parallel by using
// an  HTTP Range request to download a chunk and write it to the correct section of the file.
func (d Downloader) parallelDownload(destinationFileName, url string, n int) error {
	log.Printf("parallel download using %d workers\n", n)

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

	var g errgroup.Group
	results := make([]io.Reader, n)
	for i := 0; i < n; i++ {
		offset := int64(i) * chunkSize
		sizeToDownload := chunkSize
		if i == n-1 {
			sizeToDownload = lastChunkSize
		}
		g.Go(func() error {
			result, err := download(url, offset, sizeToDownload)
			if err == nil {
				results[i] = result
			}
			return err
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	if err := writeToFile(io.MultiReader(results...), destinationFileName); err != nil {
		return fmt.Errorf("could not write to file: %w", err)
	}

	return nil
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

func download(url string, offset, size int64) (io.Reader, error) {
	var buf bytes.Buffer

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create get request: %w", err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+size-1))

	rangeResp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not do Range request: %w", err)
	}
	defer rangeResp.Body.Close()

	log.Println("Range Content-Length", rangeResp.Header.Get("Content-Length"))

	if _, err = io.Copy(&buf, rangeResp.Body); err != nil {
		return nil, fmt.Errorf("could not copy: %w", err)
	}

	return &buf, nil
}

func writeToFile(src io.Reader, destinationFileName string) error {
	f, err := os.OpenFile(destinationFileName, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("could not open file for writing: %w", err)
	}
	defer f.Close()

	if _, err = io.Copy(f, src); err != nil {
		return fmt.Errorf("could not copy: %w", err)
	}
	return nil
}
