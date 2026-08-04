package main

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi"
)

func NewHTTPQ() *HTTPQ {
	return &HTTPQ{
		Topics:  make(map[string]*Topic),
		Timeout: 5 * time.Second,
	}
}

func (h *HTTPQ) Handler() http.Handler {
	r := chi.NewRouter()

	r.Get("/stats", h.Stats().ServeHTTP)
	r.Get("/{topic}", h.Consume().ServeHTTP)
	r.Post("/{topic}", h.Publish().ServeHTTP)

	return r
}

func (h *HTTPQ) getOrCreateTopic(name string) *Topic {
	h.mu.Lock()
	defer h.mu.Unlock()

	topic, exists := h.Topics[name]
	if !exists {
		topic = &Topic{Name: name}
		h.Topics[name] = topic
	}

	return topic
}

func removeProducer(topic *Topic, target *waitingProducer) bool {
	for i, producer := range topic.producers {
		if producer == target {
			topic.producers = append(topic.producers[:i], topic.producers[i+1:]...)
			return true
		}
	}
	return false
}

func (h *HTTPQ) Publish() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		topicName := chi.URLParam(r, "topic")

		if topicName == "" {
			http.Error(w, "topic name is required", http.StatusBadRequest)
			return
		}

		message, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read message", http.StatusBadRequest)
			return
		}

		h.mu.Lock()
		h.TxBytes += len(message)
		h.mu.Unlock()

		topic := h.getOrCreateTopic(topicName)

		producer := &waitingProducer{
			message:   message,
			delivered: make(chan struct{}, 1),
		}

		h.mu.Lock()
		if len(topic.consumers) > 0 {
			consumer := topic.consumers[0]
			topic.consumers = topic.consumers[1:]
			h.mu.Unlock()

			consumer.message <- message
			w.WriteHeader(http.StatusOK)
			return
		}

		topic.producers = append(topic.producers, producer)
		h.mu.Unlock()

		timer := time.NewTimer(h.Timeout)
		defer timer.Stop()

		select {
		case <-producer.delivered:
			w.WriteHeader(http.StatusOK)

		case <-timer.C:
			h.mu.Lock()
			removed := removeProducer(topic, producer)
			if removed {
				h.PubFails++
			}
			h.mu.Unlock()

			if !removed {
				<-producer.delivered
				w.WriteHeader(http.StatusOK)
				return
			}

			http.Error(w, "publish timed out", http.StatusRequestTimeout)

		case <-r.Context().Done():
			h.mu.Lock()
			removeProducer(topic, producer)
			h.mu.Unlock()
		}
	})
}

func removeConsumer(topic *Topic, target *waitingConsumer) bool {
	for i, consumer := range topic.consumers {
		if consumer == target {
			topic.consumers = append(topic.consumers[:i], topic.consumers[i+1:]...)
			return true
		}
	}
	return false
}

func (h *HTTPQ) Consume() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		topicName := chi.URLParam(r, "topic")
		if topicName == "" {
			http.Error(w, "topic name is required", http.StatusBadRequest)
			return
		}

		topic := h.getOrCreateTopic(topicName)

		h.mu.Lock()

		if len(topic.producers) > 0 {
			producer := topic.producers[0]
			topic.producers = topic.producers[1:]

			h.mu.Unlock()

			numberOfBytes, err := w.Write(producer.message)

			producer.delivered <- struct{}{}

			if err != nil {
				return
			}

			h.mu.Lock()
			h.RxBytes += numberOfBytes
			h.mu.Unlock()

			return
		}

		consumer := &waitingConsumer{
			message: make(chan []byte, 1),
		}

		topic.consumers = append(topic.consumers, consumer)
		h.mu.Unlock()

		timer := time.NewTimer(h.Timeout)
		defer timer.Stop()

		select {
		case message := <-consumer.message:
			numberOfBytes, err := w.Write(message)
			if err != nil {
				return
			}

			h.mu.Lock()
			h.RxBytes += numberOfBytes
			h.mu.Unlock()

		case <-timer.C:
			h.mu.Lock()
			removed := removeConsumer(topic, consumer)
			if removed {
				h.SubFails++
			}
			h.mu.Unlock()

			if !removed {
				message := <-consumer.message

				numberOfBytes, err := w.Write(message)
				if err != nil {
					return
				}

				h.mu.Lock()
				h.RxBytes += numberOfBytes
				h.mu.Unlock()

				return
			}

			http.Error(w, "consume timed out", http.StatusRequestTimeout)

		case <-r.Context().Done():
			h.mu.Lock()
			removeConsumer(topic, consumer)
			h.mu.Unlock()
		}
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
