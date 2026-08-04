package main

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi"
)

type Topic struct {
	Name string
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

func NewHTTPQ() *HTTPQ {
	return &HTTPQ{
		Topics:  make(map[string]*Topic),
		Timeout: 30 * time.Second,
	}
}

func (h *HTTPQ) Handler() http.Handler {
	r := chi.NewRouter()

	r.Get("/stats", h.Stats().ServeHTTP)
	r.Get("/{topic}", h.Consume().ServeHTTP)
	r.Post("/{topic}", h.Publish().ServeHTTP)

	return r
}

func (h *HTTPQ) Publish() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

	})
}

func (h *HTTPQ) Consume() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

	})
}

func (h *HTTPQ) Stats() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()

		stats := struct {
			RxBytes  int `json:"rxBytes"`
			TxBytes  int `json:"txBytes"`
			PubFails int `json:"pubFails"`
			SubFails int `json:"subFails"`
		}{
			RxBytes:  h.RxBytes,
			TxBytes:  h.TxBytes,
			PubFails: h.PubFails,
			SubFails: h.SubFails,
		}

		h.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		if err := json.NewEncoder(w).Encode(stats); err != nil {
			http.Error(w, "Failed to encode statistics", http.StatusInternalServerError)
		}
	})
}
