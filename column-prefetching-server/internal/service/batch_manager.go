package service

import (
	projectconfig "column-prefetching-server/internal/project-config"
	"context"
	"fmt"
	"time"
)

func NewBatchManager(
	prefetchingService *PrefetchingService,
	cfg projectconfig.BatchConfig,
) *BatchManager {
	return &BatchManager{
		prefetchingService: prefetchingService,
		batches:            make(map[string]*batch),
		batchTimeout:       cfg.BatchTimeout,
	}
}

func (manager *BatchManager) AddRequest(request PrefetchRequest) {
	batchKey := fmt.Sprintf("%s:%s", request.Bucket, request.Prefix)
	manager.mu.Lock()
	// check if the batchKey (bucket:prefix) does not exist - create a new batch for it if so
	currBatch, exists := manager.batches[batchKey]
	if !exists {
		currBatch = &batch{
			bucket:     request.Bucket,
			prefix:     request.Prefix,
			columnsSet: make(map[string]struct{}),
		}
		manager.batches[batchKey] = currBatch
	}
	manager.mu.Unlock()

	currBatch.mu.Lock()
	defer currBatch.mu.Unlock()

	// add the columns to the batch column set if not already in it
	for _, column := range request.Columns {
		if _, exists := currBatch.columnsSet[column]; !exists {
			currBatch.columnsSet[column] = struct{}{}
		}
	}

	// stop the timer since we have received a new request
	if currBatch.timer != nil {
		currBatch.timer.Stop()
	}

	// create a new timer for the specified period of 'inactivity', processing the batch if it expires
	currBatch.timer = time.AfterFunc(manager.batchTimeout, func() {
		manager.processBatch(batchKey)
	})

}

func (manager *BatchManager) processBatch(batchKey string) {
	manager.mu.Lock()

	// check that the batch key in question hasn't already been deleted (by another goroutine). If it has, then return.
	currBatch, exists := manager.batches[batchKey]
	if !exists {
		manager.mu.Unlock()
		return
	}
	delete(manager.batches, batchKey)
	manager.mu.Unlock()

	currBatch.mu.Lock()
	// convert the set of columns to a list of columns so it can be sent to prefetching service
	columns := make([]string, 0, len(currBatch.columnsSet))
	for column := range currBatch.columnsSet {
		columns = append(columns, column)
	}
	currBatch.mu.Unlock()

	// TODO: probably want to tweak this context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prefetchRequest := PrefetchRequest{
		Bucket:  currBatch.bucket,
		Prefix:  currBatch.prefix,
		Columns: columns,
	}

	startTime := time.Now()
	manager.prefetchingService.PrefetchColumns(ctx, prefetchRequest)
	elapsedTime := time.Since(startTime)

	fmt.Printf("Prefetching took: %f seconds for the following columns: %s \n", elapsedTime.Seconds(), columns)
}
