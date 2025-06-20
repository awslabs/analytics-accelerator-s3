package service

import (
	"bytes"
	projectconfig "column-prefetching-server/internal/project-config"
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
	"time"
)

func NewS3Service(cfg projectconfig.S3Config) (*S3Service, error) {
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
		config:   cfg,
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

		startTime := time.Now()
		output, err = objectPaginator.NextPage(ctx)
		elapsedTime := time.Since(startTime)

		AddDurationToTotalS3CPUTime(elapsedTime)

		if err != nil {
			var noBucket *types.NoSuchBucket

			if errors.As(err, &noBucket) {
				log.Printf("bucket %s does not exist.\n", bucket)
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
	oneMB := int64(1024 * 1024)

	rangeStart := fileSize - oneMB
	rangeEnd := fileSize - 1

	rangeHeader := fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)

	// Make request for only the last 1 MB of the Parquet file. This contains all necessary data to retrieve the footer.
	startTime := time.Now()
	result, _ := service.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	elapsedTime := time.Since(startTime)

	AddDurationToTotalS3CPUTime(elapsedTime)

	defer result.Body.Close()

	lastMB, _ := io.ReadAll(result.Body)

	// verify last 4 bytes are equal to PAR1
	parquetMagic := []byte{0x50, 0x41, 0x52, 0x31}
	if !bytes.Equal(lastMB[len(lastMB)-4:], parquetMagic) {
		return nil, fmt.Errorf("invalid Parquet magic string at end of file %s/%s. Expected %x, got %x", bucket, key, parquetMagic, lastMB[4:])
	}

	footerLengthBytes := lastMB[len(lastMB)-8 : len(lastMB)-4]
	var footerLength int32

	buf := bytes.NewReader(footerLengthBytes)
	err := binary.Read(buf, binary.LittleEndian, &footerLength)

	if err != nil {
		return nil, fmt.Errorf("failed to read footer content: %w", err)
	}

	// handles the case where the file is less than 1 MB for whatever reason
	actualDataLength := int64(len(lastMB))

	// now we have footerLength, we can make retrieve the actual footer content
	footerStart := actualDataLength - 8 - int64(footerLength)
	footerEnd := actualDataLength - 8

	footerContent := lastMB[footerStart:footerEnd]
	fileMetadata, _ := metadata.NewFileMetaData(footerContent, nil)

	return fileMetadata, nil
}

func (service *S3Service) GetColumnData(ctx context.Context, bucket string, key string, requestedColumn requestedColumn) (parquetColumnData, error) {
	rangeHeader := fmt.Sprintf("bytes=%d-%d", requestedColumn.start, requestedColumn.end)

	startTime := time.Now()
	columnDataResult, _ := service.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	elapsedTime := time.Since(startTime)

	AddDurationToTotalS3CPUTime(elapsedTime)

	defer columnDataResult.Body.Close()

	columnDataBytes, _ := io.ReadAll(columnDataResult.Body)

	parquetColumnData := parquetColumnData{
		bucket:      bucket,
		key:         key,
		column:      requestedColumn.columnName,
		data:        columnDataBytes,
		etag:        *columnDataResult.ETag,
		columnRange: fmt.Sprintf("%d-%d", requestedColumn.start, requestedColumn.end),
	}

	return parquetColumnData, nil
}
