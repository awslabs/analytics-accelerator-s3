package service

import (
	project_config "column-prefetching-server/internal/project-config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/valkey-io/valkey-glide/go/api"
)

// Service types

type PrefetchingService struct {
	S3Service    *S3Service
	CacheService *CacheService
	Config       project_config.PrefetchingConfig
}

type S3Service struct {
	S3Client *s3.Client
	Config   project_config.S3Config
}

type CacheService struct {
	ElastiCacheClient api.GlideClusterClientCommands
	Config            project_config.CacheConfig
}

// Request / Response types

type PrefetchRequest struct {
	Bucket  string
	Prefix  string
	Columns []string
}

type RequestedColumn struct {
	ColumnName string
	Start      int64
	End        int64
}
type ParquetColumnData struct {
	Bucket string
	Key    string
	Column string
	Data   []byte
	Etag   string
	Range  string
}

// Worker job types

type FileJob struct {
	Bucket    string
	File      types.Object
	ColumnSet map[string]struct{}
}

type ColumnJob struct {
	Bucket          string
	FileKey         string
	RequestedColumn RequestedColumn
}
