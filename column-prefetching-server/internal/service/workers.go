package service

import (
	"context"
	"log"
	"sync"
)

func (service *PrefetchingService) fileWorker(ctx context.Context, jobs <-chan fileJob, wg *sync.WaitGroup) {
	defer wg.Done()

	for j := range jobs {
		err := service.prefetchFileColumns(ctx, j.bucket, j.file, j.columnSet)
		if err != nil {
			log.Printf("failed to process parquet file %q: %v", j.file, err)
		}
	}
}

// columnWorker is responsible for sending the required work to the S3Service and CacheService to prefetch the
// requested column data from the parquet file, storing it in the cache.
func (service *PrefetchingService) columnWorker(ctx context.Context, jobs <-chan columnJob, wg *sync.WaitGroup) {
	defer wg.Done()

	for j := range jobs {
		columnData, _ := service.s3Service.GetColumnData(ctx, j.bucket, j.fileKey, j.requestedColumn)
		service.cacheService.CacheColumnData(columnData)
	}
}
