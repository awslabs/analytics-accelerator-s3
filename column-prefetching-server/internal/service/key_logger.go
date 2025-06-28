package service

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

type KeyLogEntry struct {
	Key       string    `json:"key"`
	Timestamp time.Time `json:"timestamp"`
	Operation string    `json:"operation"`
}

type KeyLogger struct {
	file   *os.File
	mu     sync.Mutex
	encoder *json.Encoder
}

func NewKeyLogger(filename string) (*KeyLogger, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &KeyLogger{
		file:    file,
		encoder: json.NewEncoder(file),
	}, nil
}

func (kl *KeyLogger) LogKey(key string) {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	entry := KeyLogEntry{
		Key:       key,
		Timestamp: time.Now(),
		Operation: "SET",
	}

	kl.encoder.Encode(entry)
	kl.file.Sync()
}

func (kl *KeyLogger) Close() error {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	return kl.file.Close()
}