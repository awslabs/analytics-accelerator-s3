package service

import (
	project_config "column-prefetching-server/internal/project-config"
	"fmt"
	"github.com/valkey-io/valkey-glide/go/api"
)

type CacheService struct {
	elastiCacheClient api.GlideClusterClientCommands
}

func NewCacheService(cfg project_config.CacheConfig) (*CacheService, error) {
	host := cfg.ElastiCacheEndpoint
	port := cfg.ElastiCachePort

	config := api.NewGlideClusterClientConfiguration().
		WithAddress(&api.NodeAddress{Host: host, Port: port}).
		WithUseTLS(true).
		WithRequestTimeout(5000)

	client, err := api.NewGlideClusterClient(config)

	if err != nil {
		return nil, err
	}

	return &CacheService{
		elastiCacheClient: client,
	}, nil
}

func (service *CacheService) CacheColumnData(data ParquetColumnData) error {
	cacheKey := generateCacheKey(data)
	//startTime := time.Now()
	_, err := service.elastiCacheClient.Set(cacheKey, string(data.Data))
	//elapsedTime := time.Since(startTime)
	//fmt.Printf("Set operation for key '%s' took: %s\n", cacheKey, elapsedTime)

	if err != nil {
		return err
	}

	return nil
}

func generateCacheKey(data ParquetColumnData) string {
	s3URI := fmt.Sprintf("s3://%s/%s", data.Bucket, data.Key)
	return fmt.Sprintf("%s#%s#%s", s3URI, data.Etag, data.Range)
}
