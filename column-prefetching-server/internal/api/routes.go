package api

import (
	"column-prefetching-server/internal/service"
	"net/http"
	"sync"
)

type API struct {
	prefetchingService *service.PrefetchingService
	batchManager       *service.BatchManager
	prefetchCache      sync.Map
}

func NewAPI(prefetchingService *service.PrefetchingService, batchManager *service.BatchManager) *API {
	return &API{
		prefetchingService: prefetchingService,
		batchManager:       batchManager,
	}
}

func (api *API) SetupRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/prefetch", api.HandlePrefetchColumns)
	mux.HandleFunc("DELETE /api/cache", api.HandleClearCache)

	return mux
}
