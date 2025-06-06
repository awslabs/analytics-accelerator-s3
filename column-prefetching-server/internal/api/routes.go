package api

import (
	"column-prefetching-server/internal/service"
	"net/http"
)

type API struct {
	PrefetchingService *service.PrefetchingService
}

func NewAPI(prefetchingService *service.PrefetchingService) *API {
	return &API{
		PrefetchingService: prefetchingService,
	}
}

func (api *API) SetupRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/prefetch/", api.HandlePrefetchColumns)

	return mux
}
