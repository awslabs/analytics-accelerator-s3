package service

import (
	"sync/atomic"
	"time"
)

// tracks the total sequential time spend making GET and LIST requests to S3,
// the sum of all relevant S3 request time amongst all goroutines.
var totalS3RequestCPUTime int64

// Tracks the total sequential time spent making SET requests to ElastiCache,
// the sum of all relevant ElastiCache request time amongst all goroutines.
var totalCacheRequestCPUTime int64

func AddDurationToTotalS3CPUTime(time time.Duration) {
	atomic.AddInt64(&totalS3RequestCPUTime, time.Milliseconds())
}

func AddDurationToTotalCacheCPUTime(time time.Duration) {
	atomic.AddInt64(&totalCacheRequestCPUTime, time.Milliseconds())
}

func GetTotalS3CPUTime() int64 {
	return atomic.LoadInt64(&totalS3RequestCPUTime) / 1000
}

func GetTotalCacheCPUTime() int64 {
	return atomic.LoadInt64(&totalCacheRequestCPUTime) / 1000
}
