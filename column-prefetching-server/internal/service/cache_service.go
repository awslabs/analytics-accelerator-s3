package service

import (
	project_config "column-prefetching-server/internal/project-config"
	"fmt"
	"github.com/valkey-io/valkey-glide/go/api"
	"github.com/valkey-io/valkey-glide/go/api/options"
	"time"
)

func NewCacheService(cfg project_config.CacheConfig) (*CacheService, error) {
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
		ElastiCacheClient: client,
		Config:            cfg,
	}, nil
}

func (service *CacheService) CacheColumnData(data ParquetColumnData) error {
	cacheKey := generateCacheKey(data)

	startTime := time.Now()

	//TODO: the following is how we would batch SET to cache
	//_, err := service.ElastiCacheClient.MSet(map[string]string{
	//	cacheKey: string(data.Data),
	//})

	expiry := options.NewExpiry().SetType(options.Seconds).SetCount(uint64(service.Config.TimeToLive))
	setOptions := options.NewSetOptions().SetExpiry(expiry)

	//fmt.Printf("time to live is: %d seconds \n", expiry.Count)

	_, err := service.ElastiCacheClient.SetWithOptions(cacheKey, string(data.Data), *setOptions)
	elapsedTime := time.Since(startTime)

	AddDurationToTotalCacheCPUTime(elapsedTime)

	if err != nil {
		return err
	}

	return nil
}

func generateCacheKey(data ParquetColumnData) string {
	s3URI := fmt.Sprintf("s3://%s/%s", data.Bucket, data.Key)
	return fmt.Sprintf("%s#%s#%s", s3URI, data.Etag, data.Range)
}
