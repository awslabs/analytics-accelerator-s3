package service

import (
	projectconfig "column-prefetching-server/internal/project-config"
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"
	"sort"
	"strings"
	"sync"
)

func NewPrefetchingService(
	s3Service *S3Service,
	cacheService *CacheService,
	cfg projectconfig.PrefetchingConfig,
) *PrefetchingService {
	return &PrefetchingService{
		s3Service:    s3Service,
		cacheService: cacheService,
		config:       cfg,
	}
}

// PrefetchColumns is the primary entrypoint function of the service. It is responsible for orchestrating the
// prefetching process, fetching the requested columns from S3 and caching them in ElastiCache. It does this by first
// getting a list of all parquet files in the requested location, then it processes each file in parallel, fetching the
// requested columns and storing them in ElastiCache. The service is configured with a concurrency limit, which is the
// number of concurrent goroutines that will be used to process the files.
func (service *PrefetchingService) PrefetchColumns(ctx context.Context, req PrefetchRequest) error {
	files, err := service.s3Service.ListParquetFiles(ctx, req.Bucket, req.Prefix)
	if err != nil {
		return fmt.Errorf("failed to list parquet files: %w", err)
	}
	log.Printf("Found %d parquet files to process for columns: %v", len(files), req.Columns)

	// convert slice of columns to set of columns
	columnSet := make(map[string]struct{})
	for _, column := range req.Columns {
		columnSet[column] = struct{}{}
	}

	jobs := make(chan fileJob, len(files))

	var wg sync.WaitGroup
	for i := 0; i < service.config.ConcurrencyLimit; i++ {
		wg.Add(1)
		go service.fileWorker(ctx, jobs, &wg)
	}

	for _, file := range files {

		if !strings.HasSuffix(*file.Key, ".parquet") {
			continue
		}

		jobs <- fileJob{
			bucket:    req.Bucket,
			file:      file,
			columnSet: columnSet,
		}
	}
	close(jobs)
	wg.Wait()

	fmt.Printf("Total sequential time spent making S3 Requests: %d seconds \n", GetTotalS3CPUTime())
	fmt.Printf("Total sequential time spent making ElastiCache Requests: %d seconds \n", GetTotalCacheCPUTime())

	return nil
}

// prefetchFileColumns is responsible for orchestrating the prefetching of column data for a given parquet file.
func (service *PrefetchingService) prefetchFileColumns(ctx context.Context, bucket string, file types.Object, columnSet map[string]struct{}) error {
	footerData, _ := service.s3Service.GetParquetFileFooter(ctx, bucket, *file.Key, *file.Size)

	requestedColumns, _ := getRequestedColumns(footerData, columnSet)

	jobs := make(chan columnJob, 10)

	var wg sync.WaitGroup
	for i := 0; i < service.config.ConcurrencyLimit; i++ {
		wg.Add(1)
		go service.columnWorker(ctx, jobs, &wg)
	}

	for _, requestedColumn := range requestedColumns {

		jobs <- columnJob{
			bucket:          bucket,
			fileKey:         *file.Key,
			requestedColumn: requestedColumn,
		}
	}
	close(jobs)
	wg.Wait()

	return nil
}

// getRequestedColumns is responsible for extracting the required column data as determined by the initial HTTP request.
func getRequestedColumns(footerData *metadata.FileMetaData, columnSet map[string]struct{}) ([]requestedColumn, error) {
	// a list of requested columns to be prefetched
	var requestedColumns []requestedColumn

	for _, rowGroup := range footerData.RowGroups {
		for _, columnChunk := range rowGroup.Columns {
			columnMetaData := columnChunk.MetaData

			pathInSchema := columnMetaData.PathInSchema
			columnName := pathInSchema[len(pathInSchema)-1]

			// we only want to process columns which are requested by AAL
			if _, exists := columnSet[columnName]; exists {
				if columnChunk.MetaData.DictionaryPageOffset != nil && *columnChunk.MetaData.DictionaryPageOffset != 0 {
					//	we are dealing with a dictionary
					requestedColumns = append(requestedColumns,
						requestedColumn{
							columnName: columnName,
							start:      *columnMetaData.DictionaryPageOffset,
							end:        *columnMetaData.DictionaryPageOffset + columnMetaData.TotalCompressedSize - 1,
						})
				} else {
					//	we are not dealing with a dictionary
					requestedColumns = append(requestedColumns,
						requestedColumn{
							columnName: columnName,
							start:      columnChunk.FileOffset,
							end:        columnChunk.FileOffset + columnMetaData.TotalCompressedSize - 1,
						})
				}
			}
		}
	}

	// Merge consecutive ranges to avoid making multiple small requests
	mergedRanges := mergeRanges(requestedColumns)
	
	// Combine original and merged ranges, avoiding duplicates
	allRanges := append(requestedColumns, mergedRanges...)
	deduplicatedRanges := removeDuplicateRanges(allRanges)
	
	// split ranges larger than 8MB
	return splitRanges(deduplicatedRanges), nil
}

// mergeRanges merges consecutive or overlapping ranges to avoid making multiple small requests. For example, if there are
// ranges [100-200, 500-600, 601-800, 801-900, 1000-1200], this list will be merged into [100-200,
// 500-900, 1000-1200].
func mergeRanges(ranges []requestedColumn) []requestedColumn {
	if len(ranges) == 0 {
		return ranges
	}

	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].start < ranges[j].start
	})

	var mergedRanges []requestedColumn
	i := 0
	for i < len(ranges) {
		k := i

		// while there are consecutive ranges, keep iterating
		for k < len(ranges)-1 && ranges[k].end+1 == ranges[k+1].start {
			k++
		}

		mergedRanges = append(mergedRanges, requestedColumn{
			columnName: ranges[i].columnName,
			start:      ranges[i].start,
			end:        ranges[k].end,
		})

		i = k + 1
	}

	// fmt.Println(mergedRanges)

	return mergedRanges
}

// removeDuplicateRanges removes duplicate ranges based on start and end positions
func removeDuplicateRanges(ranges []requestedColumn) []requestedColumn {
	seen := make(map[string]bool)
	var result []requestedColumn
	
	for _, r := range ranges {
		key := fmt.Sprintf("%d-%d", r.start, r.end)
		if !seen[key] {
			seen[key] = true
			result = append(result, r)
		}
	}
	
	return result
}

// splitRanges splits ranges larger than 8MB into smaller chunks
func splitRanges(ranges []requestedColumn) []requestedColumn {
	const maxRangeSize = 8 * 1024 * 1024
	var result []requestedColumn
	
	for _, r := range ranges {
		if r.end-r.start+1 > maxRangeSize {
			result = append(result, splitRange(r)...)
		} else {
			result = append(result, r)
		}
	}
	
	return result
}

// splitRange splits a single range into multiple 8MB chunks
func splitRange(r requestedColumn) []requestedColumn {
	const maxRangeSize = 8 * 1024 * 1024
	var result []requestedColumn
	
	start := r.start
	for start <= r.end {
		end := start + maxRangeSize - 1
		if end > r.end {
			end = r.end
		}
		
		result = append(result, requestedColumn{
			columnName: r.columnName,
			start:      start,
			end:        end,
		})
		
		start = end + 1
	}
	
	return result
}
