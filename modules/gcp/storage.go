package gcp

import (
	"bytes"
	"context"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/gruntwork-io/terratest/modules/logger"
)

var (
	ErrBucketNotFound = errors.New("Bucket not found")
)

// Find the name of the bucket in a project given label key=value
func FindStorageBucketWithLabel(t *testing.T, project, key, value string) string {
	bucket, err := FindStorageBucketWithLabelE(t, project, key, value)
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

// Find the name of the bucket in a project given label key=value
func FindStorageBucketWithLabelE(t *testing.T, project, key, value string) (string, error) {
	client, err := NewStorageClientE(t)
	if err != nil {
		return "", err
	}
	
	bucketIterator := client.Buckets(context.Background(), project)

	for {
		bucketAttrs, err := bucketIterator.Next()
		if err = iterator.Done {
			break
		}
		if err != nil {
			return "", err
		}
		val, ok := bucketAttrs.Labels[key]
		if !ok {
			continue
		}
		if val == value {
			bucket := bucketAttrs.Name
			logger.Logf(t, "Found Storage bucket %s with label %s=%s", bucket, key, value)
			return bucket, nil
		}
	}
	return "", ErrBucketNotFound
}

// Fetch contents of object given bucket and key, and return it as a string
func GetStorageObjectContents(t *testing.T, bucket, key string) string {
	contents, err := GetStorageObjectContentsE(t, bucket, key)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

// Fetch contents of object given bucket and key, and return it as a string
func GetStorageObjectContentsE(t *testing.T, bucket, key string) (string, error) {
	client, err := NewStorageClientE(t)
	if err != nil {
		return "", err
	}

	reader, err := client.Bucket(bucket).Object(key).NewReader(context.Background())
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	_, err = buf.ReadFrom(reader)
	if err != nil {
		return "", err
	}

	contents := buf.String()
	log.Logf(t, "Read contents from Storage Bucket: %s, %s", bucket, key)

	return contents, nil
}

// Create a Storage bucket for a project with the given name or fails the test if error
func CreateStorageBucket(t *testing.T, projectName, bucketName string) {
	err := CreateStorageBucketE(t, projectName, bucketName)
	if err != nil {
		t.Fatal(err)
	}
}

// Create a Storage bucket for a project with the given name
func CreateStorageBucketE(t *testing.T, project, bucket string) error {
	logger.Logf(t, "Creating bucket %s", bucket)

	client, err := NewStorageClientE(t)
	if err != nil {
		return err
	}

	bucketHandle := client.Bucket(bucket)
	attrs := &storage.BucketAttrs{
		Name: bucket,
	}
	return bucketHandle.Create(context.Background(), project, attrs)
}

// Destroy the Storage bucket with the given name or fail the test if there's an error
func DeleteStorageBucket(t *testing.T, name string) error {
	err := DeleteStorageBucketE(t, name)
	if err != nil {
		t.Fatal(err)
	}
}

// Destroy the Storage bucket with the given name
func DeleteStorageBucketE(t *testing.T, name string) error {
	logger.Logf(t, "Deleting bucket %s", name)

	client, err := NewStorageClientE(t)
	if err != nil {
		return err
	}

	bucketHandle := client.Bucket(name)
	return bucketHandle.Delete(context.Background())
}

// Checks if the given Storage bucket exists and fail the test if it does not
func AssertStorageBucketExists(t *testing.T, name string) {
	err := AssertStorageBucketExistsE(t, name)
	if err != nil {
		t.Fatal(err)
	}
}

// Checks if the given Storage bucket exists and returns an error if it does not
func AssertStorageBucketExistsE(t *testing.T, name string) error {
	client, err := NewStorageClientE(t)
	if err != nil {
		return err
	}

	bucketHandle := client.Bucket(name)
	// TODO: Determine if we want to handle the specific bucket not exist error
	// https://github.com/GoogleCloudPlatform/google-cloud-go/blob/f9b83d71e5382dcef644448a8ce423c9944c008b/storage/bucket.go#L163
	_, err = bucketHandle.Attrs(context.Background())
}

// Creates Google Cloud Storage client and fail the test if it does not
func NewStorageClient(t *testing.T) *storage.Client {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// Creates a Google Cloud Storage client or returns an error
func NewStorageClientE(t *testing.T) (*storage.Client, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return client, nil
}
