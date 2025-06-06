package project_config

import (
	"encoding/json"
	"os"
	"time"
)

type PrefetchingConfig struct {
	ConcurrencyLimit int `json:"concurrency_limit"`
}

type CacheConfig struct {
	TimeToLive          time.Duration `json:"time_to_live"`
	ElastiCacheEndpoint string        `json:"elasticache_endpoint"`
	ElastiCachePort     int           `json:"elasticache_port"`
}

type S3Config struct {
	Region string `json:"region"`
}

type Config struct {
	Prefetching PrefetchingConfig `json:"prefetching"`
	Cache       CacheConfig       `json:"cache"`
	S3          S3Config          `json:"s3"`
}

func LoadConfig(configPath string) (*Config, error) {
	jsonFile, err := os.ReadFile(configPath)

	var cfg Config

	err = json.Unmarshal(jsonFile, &cfg)

	return &cfg, err
}
