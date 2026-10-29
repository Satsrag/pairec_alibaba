package eas

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alibaba/pairec/v2/pkg/requesttrace"
)

func traceTestClient(t *testing.T, server *httptest.Server) *PredictClient {
	t.Helper()
	p := NewPredictClient(server.URL, "rank")
	p.SetToken("test-token")
	p.SetRetryCount(0)
	if err := p.Init(); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTraceSDKBodyAndRetry(t *testing.T) {
	payload := []byte{0, 1, 2, 127, 255}
	reply := []byte{255, 0, 4}
	got := make(chan http.Header, 3)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, payload) || r.URL.Path != "/api/predict/rank" {
			t.Errorf("request payload or URL changed: %v %s", err, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "test-token" || r.Header.Get("Content-Type") != "application/octet-stream" {
			t.Error("authentication or payload content type changed")
		}
		got <- r.Header.Clone()
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Write(reply)
	}))
	defer server.Close()
	p := traceTestClient(t, server)
	p.SetRetryCount(1)
	p.AddHeader("X-Static", "unchanged")
	trace := requesttrace.Trace{RequestID: "request-a", BatchID: "rank-9"}
	result, err := p.BytesPredictWithTrace(payload, trace)
	if err != nil || !bytes.Equal(result, reply) || calls.Load() != 2 {
		t.Fatalf("retry/response behavior changed: result=%v err=%v calls=%d", result, err, calls.Load())
	}
	for i := 0; i < 2; i++ {
		h := <-got
		if h.Get(requesttrace.RequestIDHeader) != trace.RequestID || h.Get(requesttrace.BatchIDHeader) != trace.BatchID || h.Get("X-Static") != "unchanged" {
			t.Errorf("correlation did not survive retry: %v", h)
		}
	}
	if !reflect.DeepEqual(p.headers, map[string]string{"X-Static": "unchanged"}) {
		t.Fatal("trace mutated shared client headers")
	}
	if _, err := p.BytesPredict(payload); err != nil {
		t.Fatal(err)
	}
	h := <-got
	if h.Get(requesttrace.RequestIDHeader) != "" || h.Get(requesttrace.BatchIDHeader) != "" {
		t.Fatal("trace leaked into the next legacy request")
	}
}

func TestTraceSDKConcurrentIsolation(t *testing.T) {
	const count = 24
	seen := make(map[string]bool, count)
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		id := string(body)
		if r.Header.Get(requesttrace.RequestIDHeader) != id || r.Header.Get(requesttrace.BatchIDHeader) != "batch-"+id {
			t.Errorf("concurrent request IDs mixed: body=%q request=%q batch=%q", id, r.Header.Get(requesttrace.RequestIDHeader), r.Header.Get(requesttrace.BatchIDHeader))
		}
		mu.Lock()
		seen[id] = true
		mu.Unlock()
		w.Write(body)
	}))
	defer server.Close()
	p := traceTestClient(t, server)
	p.AddHeader("X-Static", "unchanged")
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("request-%d", i)
			result, err := p.BytesPredictWithTrace([]byte(id), requesttrace.Trace{RequestID: id, BatchID: "batch-" + id})
			if err != nil || string(result) != id {
				t.Errorf("concurrent response changed: %s %v", result, err)
			}
		}(i)
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != count || len(p.headers) != 1 {
		t.Fatal("lost requests or shared headers mutated")
	}
}

func TestTraceSDKInvalidAndLegacyHeaders(t *testing.T) {
	headers := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Clone()
		w.Write([]byte("ok"))
	}))
	defer server.Close()
	p := traceTestClient(t, server)
	p.AddHeader(requesttrace.RequestIDHeader, "legacy-request")
	p.AddHeader(requesttrace.BatchIDHeader, "legacy-batch")
	if _, err := p.BytesPredict([]byte("body")); err != nil {
		t.Fatal(err)
	}
	got := <-headers
	if got.Get(requesttrace.RequestIDHeader) != "legacy-request" || got.Get(requesttrace.BatchIDHeader) != "legacy-batch" {
		t.Fatal("legacy AddHeader behavior changed")
	}
	for _, bad := range []string{"", "bad\r\nX-Injected:yes", "用户"} {
		if _, err := p.BytesPredictWithTrace([]byte("body"), requesttrace.Trace{RequestID: bad, BatchID: bad}); err != nil {
			t.Fatal(err)
		}
		got = <-headers
		if got.Get(requesttrace.RequestIDHeader) != "" || got.Get(requesttrace.BatchIDHeader) != "" || got.Get("X-Injected") != "" {
			t.Fatal("invalid trace inherited static IDs or injected a header")
		}
	}
	if p.headers[requesttrace.RequestIDHeader] != "legacy-request" {
		t.Fatal("per-request suppression mutated legacy static headers")
	}
}
