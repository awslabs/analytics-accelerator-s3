package api

import (
	"column-prefetching-server/internal/service"
	"encoding/json"
	"fmt"
	"net/http"
)

// ColumnPrefetchRequest represents the format of the incoming HTTP request from ALL.
type ColumnPrefetchRequest struct {
	Bucket  string   `json:"bucket"`
	Prefix  string   `json:"prefix"`
	Columns []string `json:"columns"`
}

func (api *API) HandlePrefetchColumns(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var apiReq ColumnPrefetchRequest

	err := json.NewDecoder(r.Body).Decode(&apiReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	if apiReq.Bucket == "" {
		http.Error(w, "bucket field is missing", http.StatusBadRequest)
		return
	}

	if apiReq.Prefix == "" {
		http.Error(w, "prefix field is missing", http.StatusBadRequest)
		return
	}

	if len(apiReq.Columns) == 0 {
		http.Error(w, "columns field is missing", http.StatusBadRequest)
		return
	}

	newColumns := api.getNewColumnsToFetch(apiReq)

	// return response if there are no new columns to fetch
	if len(newColumns) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}

	// send a response and start fetching in goroutine so the process is non-blocking
	w.WriteHeader(http.StatusAccepted)

	go func() {
		prefetchRequest := service.PrefetchRequest{
			Bucket:  apiReq.Bucket,
			Prefix:  apiReq.Prefix,
			Columns: newColumns,
		}

		// send to batch manager
		api.batchManager.AddRequest(prefetchRequest)
	}()
}

// getNewColumnsToFetch checks against prefetchCache to filter out columns that have already been prefetched in a prior
// request for a given bucket and prefix.
func (api *API) getNewColumnsToFetch(apiReq ColumnPrefetchRequest) []string {
	cacheKey := fmt.Sprintf("%s:%s", apiReq.Bucket, apiReq.Prefix)

	var prefetchedColumnsSet map[string]struct{}

	if val, found := api.prefetchCache.Load(cacheKey); found {
		prefetchedColumnsSet = val.(map[string]struct{})
	} else {
		prefetchedColumnsSet = make(map[string]struct{})
	}

	var newColumns []string
	for _, col := range apiReq.Columns {
		if _, exists := prefetchedColumnsSet[col]; !exists {
			newColumns = append(newColumns, col)
			prefetchedColumnsSet[col] = struct{}{}
		}
	}

	api.prefetchCache.Store(cacheKey, prefetchedColumnsSet)
	return newColumns
}
