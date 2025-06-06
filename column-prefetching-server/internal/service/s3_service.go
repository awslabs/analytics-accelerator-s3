package service

import (
	"bytes"
	"column-prefetching-server/internal/project-config"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
	"log"
)

type S3Service struct {
	s3Client *s3.Client
}

func NewS3Service(cfg project_config.S3Config) (*S3Service, error) {
	ctx := context.Background()

	sdkConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
	)

	if err != nil {
		return nil, err
	}

	s3Client := s3.NewFromConfig(sdkConfig)

	return &S3Service{
		s3Client: s3Client,
	}, nil
}

func (service *S3Service) ListParquetFiles(ctx context.Context, bucket string, prefix string) ([]types.Object, error) {
	var err error
	var output *s3.ListObjectsV2Output
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	var objects []types.Object
	objectPaginator := s3.NewListObjectsV2Paginator(service.s3Client, input)

	for objectPaginator.HasMorePages() {
		output, err = objectPaginator.NextPage(ctx)
		if err != nil {
			var noBucket *types.NoSuchBucket

			if errors.As(err, &noBucket) {
				log.Printf("Bucket %s does not exist.\n", bucket)
				err = noBucket
			}
			break

		} else {
			objects = append(objects, output.Contents...)
		}
	}

	return objects, err
}

func (service *S3Service) GetParquetFileFooter(ctx context.Context, bucket string, key string, fileSize int64) (*metadata.FileMetaData, error) {
	rangeStart := fileSize - 8
	rangeEnd := fileSize - 1

	rangeHeader := fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)

	// make request for only the last 8 bytes of the Parquet file
	result, _ := service.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})

	defer result.Body.Close()

	footerBytes, _ := io.ReadAll(result.Body)

	// verify last 4 bytes are equal to PAR1
	parquetMagic := []byte{0x50, 0x41, 0x52, 0x31}
	if !bytes.Equal(footerBytes[4:], parquetMagic) {
		return nil, fmt.Errorf("invalid Parquet magic string at end of file %s/%s. Expected %x, got %x", bucket, key, parquetMagic, footerBytes[4:])
	}

	footerLengthBytes := footerBytes[:4]
	var footerLength int32

	buf := bytes.NewReader(footerLengthBytes)
	err := binary.Read(buf, binary.LittleEndian, &footerLength)

	// now we have footerLength, we can make the S3 request to get actual footer content

	rangeStart = fileSize - 8 - int64(footerLength)
	rangeEnd = fileSize - 9
	rangeHeader = fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)

	footerResult, _ := service.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})

	defer footerResult.Body.Close()

	footerContent, err := io.ReadAll(footerResult.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read footer content: %w", err)
	}

	fileMetadata, _ := metadata.NewFileMetaData(footerContent, nil)

	return fileMetadata, nil
}

func (service *S3Service) GetColumnData(ctx context.Context, bucket string, key string, requestedColumn RequestedColumn) (ParquetColumnData, error) {
	rangeHeader := fmt.Sprintf("bytes=%d-%d", requestedColumn.Start, requestedColumn.End)

	columnDataResult, _ := service.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})

	defer columnDataResult.Body.Close()

	columnDataBytes, _ := io.ReadAll(columnDataResult.Body)

	parquetColumnData := ParquetColumnData{
		Bucket: bucket,
		Key:    key,
		Column: requestedColumn.ColumnName,
		Data:   columnDataBytes,
		Etag:   *columnDataResult.ETag,
		Range:  rangeHeader,
	}

	return parquetColumnData, nil
}
