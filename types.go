package main

import (
	"sync"
	"time"
)

type waitingProducer struct {
	message   []byte
	delivered chan struct{}
}

type waitingConsumer struct {
	message chan []byte
}

type Topic struct {
	Name      string
	producers []*waitingProducer
	consumers []*waitingConsumer
}

type HTTPQ struct {
	RxBytes  int // number of bytes (message body) consumed
	TxBytes  int // number of bytes (message body) published
	PubFails int // number of publish failures
	SubFails int // number of subscribe failures

	Topics  map[string]*Topic // all topics
	Timeout time.Duration     // timeout setting
	mu      sync.Mutex        // concurrency protection
}
