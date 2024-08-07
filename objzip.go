package objzip

import (
	"compress/gzip"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/ptr"
	"github.com/pkg/errors"
)

type Client struct {
	getter   Getter
	uploader Uploader
}

type Getter interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// Uploader is the upload component of the S3 client.
// uploader := manager.NewUploader(s3Client)
type Uploader interface {
	Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

func New(g Getter, u Uploader) *Client {
	return &Client{
		getter:   g,
		uploader: u,
	}
}

func (c *Client) ReadAndUnzip(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	out, err := c.getter.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr.String(bucket),
		Key:    ptr.String(key),
	})
	if err != nil {
		return nil, err
	}
	wantZipped := true
	if !wantZipped {
		return out.Body, nil
	}

	zipReader, err := gzip.NewReader(out.Body)
	if err != nil {
		return nil, err
	}

	return struct {
		io.Reader
		io.Closer
	}{
		Reader: zipReader,
		Closer: out.Body,
	}, nil
}

func (c *Client) ZipAndWrite(ctx context.Context, bucket, key string, rc io.Reader) error {
	pipeR, pipeW := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		compressed := gzip.NewWriter(pipeW)
		if _, err := io.Copy(compressed, rc); err != nil {
			errCh <- errors.Wrap(err, "failed to copy to gzip writer")
			return
		}
		if err := compressed.Close(); err != nil {
			errCh <- err
			return
		}
		if err := pipeW.Close(); err != nil {
			errCh <- err
			return
		}
	}()

	if _, uploadErr := c.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: ptr.String(bucket),
		Key:    ptr.String(key),
		Body:   pipeR,
	}); uploadErr != nil {
		return uploadErr
	}
	if err := pipeR.Close(); err != nil {
		return err
	}
	return <-errCh
}
