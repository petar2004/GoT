package main_test

import (
	httpq "GoT"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type statsResponse struct {
	RxBytes  int `json:"rxBytes"`
	TxBytes  int `json:"txBytes"`
	PubFails int `json:"pubFails"`
	SubFails int `json:"subFails"`
}

func getStats(t *testing.T, handler http.Handler) statsResponse {
	t.Helper()

	request := httptest.NewRequest(http.MethodGet, "/stats", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var stats statsResponse

	err := json.NewDecoder(recorder.Body).Decode(&stats)
	if err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}

	return stats
}

func TestPublish(t *testing.T) {
	httpQ := httpq.NewHTTPQ()
	httpQ.Timeout = 50 * time.Millisecond

	message := "Testing...."

	request := httptest.NewRequest(http.MethodPost, "/schneider", strings.NewReader(message))
	recorder := httptest.NewRecorder()

	handler := httpQ.Handler()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusRequestTimeout {
		t.Fatalf("expected status %d, got %d", http.StatusRequestTimeout, recorder.Code)
	}

	stats := getStats(t, handler)

	if stats.RxBytes != 0 {
		t.Errorf("expected rxBytes %d, got %d", 0, stats.RxBytes)
	}

	if stats.TxBytes != len(message) {
		t.Errorf("expected txBytes %d, got %d", len(message), stats.TxBytes)
	}

	if stats.PubFails != 1 {
		t.Errorf("expected pubFails %d, got %d", 1, stats.PubFails)
	}

	if stats.SubFails != 0 {
		t.Errorf("expected subFails %d, got %d", 0, stats.SubFails)
	}
}

func TestConsume(t *testing.T) {
	httpQ := httpq.NewHTTPQ()
	httpQ.Timeout = 50 * time.Millisecond

	request := httptest.NewRequest(http.MethodGet, "/schneider", nil)
	recorder := httptest.NewRecorder()

	handler := httpQ.Handler()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusRequestTimeout {
		t.Errorf("expected status %d, got %d", http.StatusRequestTimeout, recorder.Code)
	}

	stats := getStats(t, handler)

	if stats.RxBytes != 0 {
		t.Errorf("expected rxBytes %d, got %d", 0, stats.RxBytes)
	}

	if stats.TxBytes != 0 {
		t.Errorf("expected txBytes %d, got %d", 0, stats.TxBytes)
	}

	if stats.PubFails != 0 {
		t.Errorf("expected pubFails %d, got %d", 0, stats.PubFails)
	}

	if stats.SubFails != 1 {
		t.Errorf("expected subFails %d, got %d", 1, stats.SubFails)
	}
}

func TestPublishAndConsume(t *testing.T) {
	httpQ := httpq.NewHTTPQ()
	httpQ.Timeout = 1 * time.Second

	message := "Testing...."

	publishRequest := httptest.NewRequest(http.MethodPost, "/schneider", strings.NewReader(message))
	publishRecorder := httptest.NewRecorder()

	handler := httpQ.Handler()

	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		handler.ServeHTTP(publishRecorder, publishRequest)
	}()

	consumeRequest := httptest.NewRequest(http.MethodGet, "/schneider", nil)
	consumeRecorder := httptest.NewRecorder()

	handler.ServeHTTP(consumeRecorder, consumeRequest)

	if consumeRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, consumeRecorder.Code)
	}

	if consumeRecorder.Body.String() != message {
		t.Fatalf("expected message %s, got %s", message, consumeRecorder.Body.String())
	}

	select {
	case <-publishDone:

	case <-time.After(2 * time.Second):
		t.Fatalf("publish did not finish in time")
	}

	if publishRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, publishRecorder.Code)
	}

	stats := getStats(t, handler)

	if stats.RxBytes != len(message) {
		t.Errorf("expected rxBytes %d, got %d", len(message), stats.RxBytes)
	}

	if stats.TxBytes != len(message) {
		t.Errorf("expected txBytes %d, got %d", len(message), stats.TxBytes)
	}

	if stats.PubFails != 0 {
		t.Errorf("expected pubFails %d, got %d", 0, stats.PubFails)
	}

	if stats.SubFails != 0 {
		t.Errorf("expected subFails %d, got %d", 0, stats.SubFails)
	}
}
