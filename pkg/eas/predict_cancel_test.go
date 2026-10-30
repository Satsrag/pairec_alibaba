package eas

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alibaba/pairec/v2/pkg/requesttrace"
)

func TestTraceSDKCallerCancellation(t *testing.T) {
	for _, phase := range []string{"before dispatch", "waiting headers", "reading body"} {
		t.Run(phase, func(t *testing.T) {
			var calls atomic.Int32
			started, disconnected := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				calls.Add(1)
				if phase == "reading body" {
					w.Write([]byte("partial"))
					w.(http.Flusher).Flush()
				}
				close(started)
				<-r.Context().Done()
				close(disconnected)
			}))
			defer server.Close()
			p := traceTestClient(t, server)
			p.SetRetryCount(3)
			caller, cancel := context.WithCancel(context.Background())
			defer cancel()
			if phase == "before dispatch" {
				cancel()
			}
			done := make(chan error, 1)
			go func() {
				_, err := p.BytesPredictWithTrace([]byte{0, 255}, requesttrace.Trace{RequestID: "cancel", BatchID: "rank-1", Context: caller})
				done <- err
			}()
			if phase != "before dispatch" {
				select {
				case <-started:
				case <-time.After(2 * time.Second):
					t.Fatal("HTTP request never reached backend")
				}
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("expected caller cancellation, got %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("canceled HTTP call did not return")
			}
			if phase == "before dispatch" {
				if calls.Load() != 0 {
					t.Fatal("pre-canceled caller sent HTTP work")
				}
			} else {
				select {
				case <-disconnected:
				case <-time.After(2 * time.Second):
					t.Fatal("backend did not observe disconnect")
				}
				if calls.Load() != 1 {
					t.Fatal("canceled caller retried HTTP work")
				}
			}
		})
	}
}

func TestTraceSDKClientTimeoutStillRetries(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if calls.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		w.Write([]byte("unchanged"))
	}))
	defer server.Close()
	p := traceTestClient(t, server)
	p.SetTimeout(100)
	p.SetRetryCount(1)
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	got, err := p.BytesPredictWithTrace([]byte("body"), requesttrace.Trace{RequestID: "active", BatchID: "rank-1", Context: caller})
	if err != nil || string(got) != "unchanged" || calls.Load() != 2 || caller.Err() != nil {
		t.Fatalf("internal timeout changed caller/retry behavior: calls=%d err=%v", calls.Load(), err)
	}
}

func TestTraceSDKCancelOneConcurrentCaller(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		id := string(body)
		if r.Header.Get(requesttrace.RequestIDHeader) != id || r.Header.Get(requesttrace.BatchIDHeader) != "batch-"+id {
			t.Error("concurrent caller metadata mixed")
		}
		if id == "canceled" {
			close(started)
			<-r.Context().Done()
			return
		}
		w.Write(body)
	}))
	defer server.Close()
	p := traceTestClient(t, server)
	p.SetRetryCount(2)
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := p.BytesPredictWithTrace([]byte("canceled"), requesttrace.Trace{RequestID: "canceled", BatchID: "batch-canceled", Context: caller})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent canceled call never reached backend")
	}
	cancel()
	got, err := p.BytesPredictWithTrace([]byte("active"), requesttrace.Trace{RequestID: "active", BatchID: "batch-active", Context: context.Background()})
	if err != nil || string(got) != "active" {
		t.Fatalf("one caller canceled another: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled caller error=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled caller did not return")
	}
}
