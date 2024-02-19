package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"

	"downloader/downloader"
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

	var numWorkers int
	if isParallel() {
		numWorkers, _ = strconv.Atoi(os.Args[2])
	}

	d := downloader.NewDownloader(isParallel(), destinationFileName, url, numWorkers)
	if err := d.Download(); err != nil {
		log.Fatalf("could not download: %s", err)
	}

	// Confirm downloaded file matches md5 signature
	err = checkSignature(destinationFileName, "9338036cc8e8a2a002c2b2c036bdd0d4")
	if err != nil {
		log.Fatalf("error checking for signature: %s", err)
	}

	log.Printf("Succesfully downloaded file to: %s", destinationFileName)
}

func validateArgs() error {
	if len(os.Args) < 2 {
		return errors.New("-s or -p are required. go run main.go -s|-p")
	}
	if !slices.Contains([]string{"-s", "-p"}, os.Args[1]) {
		return errors.New("only -s and -p are allowed. -s for sequential, -p for parallel")
	}
	if os.Args[1] == "-p" && len(os.Args) != 3 {
		return errors.New("for -p specify the number of workers. ex. go run main.go -p 8")
	}
	if os.Args[1] == "-p" {
		if _, err := strconv.Atoi(os.Args[2]); err != nil {
			return errors.New("when using -p, number of workers has to be an int. ex. go run main.go -p 8")
		}
	}
	return nil
}

func isParallel() bool {
	if len(os.Args) < 2 {
		return false
	}
	return os.Args[1] == "-p"
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
