package main

import (
	"flag"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func main() {
	bucket := flag.String("bucket", "", "The S3 bucket name to read from")
	key := flag.String("key", "", "The S3 key to read from")
	output := flag.String("output", "", "The path to write the output to")

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

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)

	// Create a file to write the S3 Object contents to.
	f, err := os.Create(*output)
	if err != nil {
		log.Fatalf("Failed to create file %q: %+v", *output, err)
	}

	// Write the contents of S3 Object to the file
	n, err := downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(*bucket),
		Key:    aws.String(*key),
	})
	if err != nil {
		log.Fatalf("Failed to download file: %+v", err)
	}
	log.Printf("File downloaded, %d bytes", n)
}
