package main

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const Etag = "be04a3eb37076d6f0c479decb4e3738e-8"

func main() {
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

	sequentialDownload(destinationFileName, url)

	// Confirm downloaded file matches md5 signature
	err = checkSignature(destinationFileName, "9338036cc8e8a2a002c2b2c036bdd0d4")
	if err != nil {
		log.Fatalf("error checking for signature: %s", err)
	}

	log.Println("All good!")
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
