package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"strings"
	"time"

	"cloud.google.com/go/storage"
)

type GCSDownloader struct {
	Url        string
	svc        *storage.Client
	reqTimeout time.Duration
}

func (gcsDownloader GCSDownloader) GetFileInfo() (int64, bool, bool) {
	bucket, object := getBucketAndObject(gcsDownloader.Url)

	ctx, cancel := context.WithTimeout(context.Background(), gcsDownloader.reqTimeout)
	defer cancel()

	attrs, err := gcsDownloader.svc.Bucket(bucket).Object(object).Attrs(ctx)
	if err != nil {
		fmt.Println("Failed to get attributes: ", err)
		return 0, false, false
	}
	return attrs.Size, true, false
}

func (gcsDownloader GCSDownloader) Get() io.ReadCloser {
	bucket, object := getBucketAndObject(gcsDownloader.Url)

	ctx, cancel := context.WithTimeout(context.Background(), gcsDownloader.reqTimeout)
	defer cancel()

	rc, err := gcsDownloader.svc.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		fmt.Println("Failed to create reader: ", err)
		return nil
	}
	return rc
}

func (gcsDownloader GCSDownloader) GetRange(start, end int64) io.ReadCloser {
	bucket, object := getBucketAndObject(gcsDownloader.Url)

	ctx, cancel := context.WithTimeout(context.Background(), gcsDownloader.reqTimeout)
	defer cancel()

	rc, err := gcsDownloader.svc.Bucket(bucket).Object(object).NewRangeReader(ctx, start, end-start)
	if err != nil {
		fmt.Println("Failed to create range reader: ", err)
		return nil
	}
	return rc
}

// GCS doesn't support multipart range requests right now, so this will never be used
// for actual file download. Still here for when they eventually do support it though.
func (gcsDownloader GCSDownloader) GetRanges(ranges [][]int64) (*multipart.Reader, error) {
	return nil, errors.New("multipart range requests not supported by GCS")
}

func getBucketAndObject(url string) (string, string) {
	parts := strings.Split(strings.Replace(url, "gs://", "", 1), "/")
	bucket := parts[0]
	object := strings.Join(parts[1:], "/")
	return bucket, object
}
