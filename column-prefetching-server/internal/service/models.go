package service

import (
	project_config "column-prefetching-server/internal/project-config"
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	"sync"
	"time"
)

// Batch stuff

type BatchManager struct {
	prefetchingService *PrefetchingService
	batches            map[string]*batch
	batchTimeout       time.Duration
	mu                 sync.Mutex
}

type batch struct {
	bucket     string
	prefix     string
	columnsSet map[string]struct{}
	timer      *time.Timer
	mu         sync.Mutex
}

// Service types

type PrefetchingService struct {
	s3Service    *S3Service
	cacheService *CacheService
	config       project_config.PrefetchingConfig
}

type S3Service struct {
	s3Client *s3.Client
	config   project_config.S3Config
}

type CacheService struct {
	elastiCacheClient *glide.ClusterClient
	config            project_config.CacheConfig
	batchRequests     chan SetRequest
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
	batcherStarted    bool
	mu                sync.Mutex
}

// Request / Response types

type PrefetchRequest struct {
	Bucket  string
	Prefix  string
	Columns []string
}

type requestedColumn struct {
	columnName string
	start      int64
	end        int64
}
type parquetColumnData struct {
	bucket      string
	key         string
	column      string
	data        []byte
	etag        string
	columnRange string
}

// Worker job types

type fileJob struct {
	bucket    string
	file      types.Object
	columnSet map[string]struct{}
}

type columnJob struct {
	bucket          string
	fileKey         string
	requestedColumn requestedColumn
}

type SetRequest struct {
	Key   string
	Value string
}
