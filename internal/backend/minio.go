package backend

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Backend interface {
	Put(ctx context.Context, bucket, key string, data io.Reader, size int64) error
	Get(ctx context.Context, bucket, key string) (io.ReadCloser, int64, error)
}

type MinioBackend struct {
	client *minio.Client
}

func NewMinio(endpoint, accessKey, secretKey string) (*MinioBackend, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("minio endpoint is empty")
	}
	ep, secure, err := normalizeEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	client, err := minio.New(ep, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, err
	}
	return &MinioBackend{client: client}, nil
}

func (m *MinioBackend) Put(ctx context.Context, bucket, key string, data io.Reader, size int64) error {
	_, err := m.client.PutObject(ctx, bucket, key, data, size, minio.PutObjectOptions{})
	return err
}

func (m *MinioBackend) Get(ctx context.Context, bucket, key string) (io.ReadCloser, int64, error) {
	obj, err := m.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, 0, err
	}
	info, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		return nil, 0, err
	}
	return obj, info.Size, nil
}

func normalizeEndpoint(endpoint string) (string, bool, error) {
	if strings.Contains(endpoint, "://") {
		u, err := url.Parse(endpoint)
		if err != nil {
			return "", false, err
		}
		secure := u.Scheme == "https"
		if u.Host == "" {
			return "", false, fmt.Errorf("invalid endpoint: %s", endpoint)
		}
		return u.Host, secure, nil
	}
	// default to http when scheme is omitted
	return endpoint, false, nil
}
