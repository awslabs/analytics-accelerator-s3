package api

import (
	"column-prefetching-server/internal/service"
	"net/http"
	"sync"
)

type API struct {
	PrefetchingService *service.PrefetchingService
	prefetchCache      sync.Map
}

func NewAPI(prefetchingService *service.PrefetchingService) *API {
	return &API{
		PrefetchingService: prefetchingService,
	}
}

func (api *API) SetupRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/prefetch", api.HandlePrefetchColumns)

	return mux
}
