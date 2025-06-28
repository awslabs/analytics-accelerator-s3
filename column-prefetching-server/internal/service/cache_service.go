package service

import (
  projectconfig "column-prefetching-server/internal/project-config"
  "context"
  "fmt"
  "time"
  "github.com/valkey-io/valkey-glide/go/v2"
  "github.com/valkey-io/valkey-glide/go/v2/config"
  "github.com/valkey-io/valkey-glide/go/v2/options"
  "github.com/valkey-io/valkey-glide/go/v2/pipeline"
)

func NewCacheService(cfg projectconfig.CacheConfig) (*CacheService, error) {
  host := cfg.ElastiCacheEndpoint
  port := cfg.ElastiCachePort

  numClients := cfg.NumClients
  
  clients := make([]*glide.ClusterClient, numClients)
  channels := make([]chan SetRequest, numClients)
  
  for i := 0; i < numClients; i++ {
    clusterConfig := config.NewClusterClientConfiguration().
      WithAddress(&config.NodeAddress{Host: host, Port: port}).
      WithUseTLS(true).
      WithRequestTimeout(cfg.RequestTimeout)
    client, err := glide.NewClusterClient(clusterConfig)
    if err != nil {
      return nil, err
    }
    clients[i] = client
    channels[i] = make(chan SetRequest, 10000)
  }

  ctx, cancel := context.WithCancel(context.Background())

  // keyLogger, _ := NewKeyLogger("elasticache_keys.json")

  service := &CacheService{
    elastiCacheClients: clients,
    config:             cfg,
    batchRequests:      channels,
    ctx:                ctx,
    cancel:             cancel,
    batcherStarted:     false,
    // keyLogger:          keyLogger,
  }

  service.startBatching()
  return service, nil
}

func (service *CacheService) CacheColumnData(data parquetColumnData) {
  cacheKey := generateCacheKey(data)
  
  channelIndex := int(service.clientIndex) % len(service.batchRequests)
  service.clientIndex++
  
  service.batchRequests[channelIndex] <- SetRequest{
    Key:   cacheKey,
    Value: string(data.data),
  }
}

func (service *CacheService) startBatching() {
  service.mu.Lock()
  defer service.mu.Unlock()

  // we only want to start the batch processor once, so return if already started
  if service.batcherStarted {
    return
  }
  service.batcherStarted = true

  // Start one processor per client/channel
  for i := 0; i < len(service.elastiCacheClients); i++ {
    service.wg.Add(1)
    go service.batchProcessor(i)
  }
}

func (service *CacheService) batchProcessor(clientID int) {
  defer service.wg.Done()

  client := service.elastiCacheClients[clientID]
  channel := service.batchRequests[clientID]

  var currentBatch *pipeline.ClusterBatch
  var setBatch []SetRequest
  timer := time.NewTimer(service.config.BatchTimeout)
  defer timer.Stop()

  // anonymous function to create a new batch of cache set operations
  resetBatch := func() {
    currentBatch = pipeline.NewClusterBatch(false)
    setBatch = make([]SetRequest, 0, service.config.BatchSize)
  }

  // anonymous function to process accumulated cache set operations
  sendBatch := func() {
    if len(setBatch) == 0 {
      return
    }

    _, err := client.Exec(service.ctx, *currentBatch, false)

    if err != nil {
      fmt.Printf("Error executing batch: %v\n", err)
      return
      }
    
    resetBatch()
  }

  // create an initial batch
  resetBatch()

  timer.Reset(service.config.BatchTimeout)

  for {
    select {
    case req := <-channel:

		// add request to set batch
      setBatch = append(setBatch, req)

	  // add the set command to the batch operation
      setOptions := options.NewSetOptions().
        SetExpiry(options.NewExpiryIn(service.config.TimeToLive))
      currentBatch.SetWithOptions(req.Key, req.Value, *setOptions)
      
      // Log the key being set
      // service.keyLogger.LogKey(req.Key)

	  // check if the batch is full, process it if so
      if len(setBatch) >= service.config.BatchSize {
        if !timer.Stop() {
          <-timer.C
        }
        sendBatch()
        timer.Reset(service.config.BatchTimeout)
      }

	  //	the timer is done, so process the batch
    case <-timer.C:
      sendBatch()
      timer.Reset(service.config.BatchTimeout)
    }
  }

}

func generateCacheKey(data parquetColumnData) string {
  s3URI := fmt.Sprintf("s3://%s/%s", data.bucket, data.key)
  return fmt.Sprintf("%s#%s#%s", s3URI, data.etag, data.columnRange)
}
