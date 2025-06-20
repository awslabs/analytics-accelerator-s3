package service

import (
	projectconfig "column-prefetching-server/internal/project-config"
	"fmt"
	"github.com/valkey-io/valkey-glide/go/api"
	"github.com/valkey-io/valkey-glide/go/api/options"
	"time"
)

func NewCacheService(cfg projectconfig.CacheConfig) (*CacheService, error) {
	// TODO: decide if we want to pass in the host and port from AAL via the HTTP request to CPS endpoint
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
		config:            cfg,
	}, nil
}

func (service *CacheService) CacheColumnData(data parquetColumnData) error {
	cacheKey := generateCacheKey(data)

	startTime := time.Now()

	//TODO: the following is how we would batch SET to cache
	//_, err := service.elastiCacheClient.MSet(map[string]string{
	//	cacheKey: string(data.data),
	//})

	expiry := options.NewExpiry().SetType(options.Seconds).SetCount(uint64(service.config.TimeToLive))
	setOptions := options.NewSetOptions().SetExpiry(expiry)

	//fmt.Printf("time to live is: %d seconds \n", expiry.Count)

	_, err := service.elastiCacheClient.SetWithOptions(cacheKey, string(data.data), *setOptions)
	elapsedTime := time.Since(startTime)

	AddDurationToTotalCacheCPUTime(elapsedTime)

	if err != nil {
		return err
	}

	return nil
}

func generateCacheKey(data parquetColumnData) string {
	s3URI := fmt.Sprintf("s3://%s/%s", data.bucket, data.key)
	return fmt.Sprintf("%s#%s#%s", s3URI, data.etag, data.columnRange)
}
