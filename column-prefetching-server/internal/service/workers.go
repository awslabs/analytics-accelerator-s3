package service

import (
	"context"
	"log"
	"sync"
)

func (service *PrefetchingService) fileWorker(ctx context.Context, jobs <-chan FileJob, wg *sync.WaitGroup) {
	defer wg.Done()

	for j := range jobs {
		err := service.prefetchFileColumns(ctx, j.Bucket, j.File, j.ColumnSet)
		if err != nil {
			log.Printf("failed to process parquet file %q: %v", j.File, err)
		}
	}
}

// columnWorker is responsible for sending the required work to the S3Service and CacheService to prefetch the
// requested column data from the parquet file, storing it in the cache.
func (service *PrefetchingService) columnWorker(ctx context.Context, jobs <-chan ColumnJob, wg *sync.WaitGroup) {
	defer wg.Done()

	for j := range jobs {
		columnData, _ := service.S3Service.GetColumnData(ctx, j.Bucket, j.FileKey, j.RequestedColumn)
		service.CacheService.CacheColumnData(columnData)
	}
}
