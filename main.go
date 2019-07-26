package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/iandees/progress"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func getFileSize(svc *s3.S3, bucket string, prefix string) (filesize int64, error error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix),
	}

	resp, err := svc.HeadObject(params)
	if err != nil {
		return 0, err
	}

	return *resp.ContentLength, nil
}

func byteCountDecimalSize(size int64) string {
	const unit = 1000
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}

	div, exp := int64(unit), 0

	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "kMGTPE"[exp])
}

func main() {
	bucket := flag.String("bucket", "", "The S3 bucket name to read from")
	key := flag.String("key", "", "The S3 key to read from")
	output := flag.String("output", "", "The path to write the output to")
	flag.Parse()

	if *bucket == "" {
		log.Fatalf("Must provide --bucket")
	}

	if *key == "" {
		log.Fatalf("Must provide --key")
	}

	if *output == "" {
		log.Fatalf("Must provide --output")
	}

	// The session the S3 Downloader will use
	sess := session.Must(session.NewSession())

	// Get the file size to help with progress
	s3Client := s3.New(sess)
	size, err := getFileSize(s3Client, *bucket, *key)
	if err != nil {
		log.Fatalf("Couldn't get file size: %+v", err)
	}

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)

	log.Printf("Starting download of s3://%s%s, size: %s", *bucket, *key, byteCountDecimalSize(size))

	// Create a file to write the S3 Object contents to.
	f, err := os.Create(*output)
	if err != nil {
		log.Fatalf("Failed to create file %q: %+v", *output, err)
	}

	// Wrap the file in a progress counter
	w := progress.NewWriterAt(f)
	// Start a ticker to write out progress
	go func() {
		ctx := context.Background()
		progressChan := progress.NewTicker(ctx, w, size, 1*time.Second)
		for p := range progressChan {
			fmt.Printf("\r%s downloaded, %v remaining...", byteCountDecimalSize(p.N()), p.Remaining().Round(time.Second))
		}
		fmt.Printf("\rdownload is complete")
	}()

	// Write the contents of S3 Object to the file
	n, err := downloader.Download(w, &s3.GetObjectInput{
		Bucket: aws.String(*bucket),
		Key:    aws.String(*key),
	})
	if err != nil {
		log.Fatalf("Failed to download file: %+v", err)
	}

	log.Printf("File downloaded, %d bytes", n)
}
