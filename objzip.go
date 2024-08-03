package objzip

import (
	"compress/gzip"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Client struct {
	s3Client PutGetter
}

type PutGetter interface {
	Putter
	Getter
}

type Putter interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type Getter interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

func New(p PutGetter) *Client {
	return &Client{s3Client: p}
}

func (c *Client) Put(ctx context.Context, body io.Reader, bucket, key string) error {
	pr, pw := io.Pipe()
	errChan := make(chan error)
	go func() {
		defer close(errChan)
		gw := gzip.NewWriter(pw)
		_, err := io.Copy(gw, body)
		if err != nil {
			errChan <- err
			return
		}
		err = gw.Close()
		if err != nil {
			errChan <- err
			return
		}
		err = pw.Close()
		if err != nil {
			errChan <- err
			return
		}
	}()
	_, err := c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   pr,
	})
	if err != nil {
		return err
	}
	return <-errChan
}

type readCloser struct {
	io.Reader
	io.Closer
}

func (c *Client) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	out, err := c.s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	zipReader, err := gzip.NewReader(out.Body)
	if err != nil {
		return nil, err
	}
	return &readCloser{
		Reader: zipReader,
		Closer: out.Body,
	}, nil
}
