package main

import (
	"column-prefetching-server/internal/api"
	"column-prefetching-server/internal/project-config"
	"column-prefetching-server/internal/service"
	"log"
	"net/http"
)

func main() {
	// Initialise project config
	cfg, _ := project_config.LoadConfig("config.json")

	// Initialise S3 service
	s3Service, err := service.NewS3Service(cfg.S3)
	if err != nil {
		log.Fatalf("Failed to create S3 service: %v", err)
	}

	// Initialise Cache service
	elastiCacheService, err := service.NewCacheService(cfg.Cache)
	if err != nil {
		log.Fatalf("Failed to create ElastiCache service: %v", err)
	}

	prefetchingService := service.NewPrefetchingService(
		s3Service,
		elastiCacheService,
		cfg.Prefetching,
	)

	apiInstance := api.NewAPI(prefetchingService)

	mux := apiInstance.SetupRoutes()

	err = http.ListenAndServe(":8080", mux)

	if err != nil {
		return
	}
}
