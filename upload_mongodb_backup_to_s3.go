package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func uploadFileToS3(s3Client *s3.S3, bucketName, filePath, key string) error {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file %s: %v", filePath, err)
	}
	defer file.Close()

	// Upload the file to S3
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   file,
		// You can set additional options like ACL, ContentType, etc. here if needed
	})
	if err != nil {
		return fmt.Errorf("error uploading file to S3: %v", err)
	}

	fmt.Printf("File %s uploaded to S3://%s/%s\n", filePath, bucketName, key)
	return nil
}

func uploadFolderToS3(s3Client *s3.S3, bucketName, folderPath string) error {
	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Determine the S3 key based on the relative path from the folder
		relPath, err := filepath.Rel(folderPath, path)
		if err != nil {
			return err
		}
		key := strings.ReplaceAll(relPath, string(filepath.Separator), "/")

		// Upload the file to S3
		if err := uploadFileToS3(s3Client, bucketName, path, key); err != nil {
			fmt.Printf("Error uploading file %s: %v\n", path, err)
		}

		return nil
	})

	return err
}

func main() {
	// AWS S3 credentials and configuration
	awsAccessKey := "xxxxxxxxxxxxxxx"
	awsSecretKey := "xxxxxxxxxxxxxxxxxxxxxxxxxxx"
	awsRegion := "us-east-1"
	bucketName := "my-bucket"
	folderPath := "/home/rolex/yadav/dump"

	// Create a new AWS session
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, ""),
	})
	if err != nil {
		fmt.Println("Error creating AWS session:", err)
		return
	}

	// Create an S3 service client
	s3Client := s3.New(sess)

	// Upload the folder to S3
	if err := uploadFolderToS3(s3Client, bucketName, folderPath); err != nil {
		fmt.Println("Error uploading folder to S3:", err)
		return
	}

	fmt.Printf("Folder %s uploaded to S3://%s\n", folderPath, bucketName)
}
