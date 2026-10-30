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

	"github.com/alibaba/pairec/v2/algorithm/eas/easyrec"
	eassdk "github.com/alibaba/pairec/v2/pkg/eas"
	"github.com/alibaba/pairec/v2/pkg/requesttrace"
	"github.com/alibaba/pairec/v2/recconf"
	"github.com/golang/protobuf/proto"
)

func cancelTestModel(t *testing.T, url, endpoint string, timeout int) *EasModel {
	t.Helper()
	m := NewEasModel("cancel-test")
	if err := m.Init(&recconf.AlgoConfig{EasConf: recconf.EasConfig{
		Processor: Eas_Processor_EASYREC, EndpointType: endpoint,
		Url: url + "/api/predict/cancel-test", Timeout: timeout, RetryTimes: 3,
	}}); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestTraceEasyRecCancellationAndIsolation(t *testing.T) {
	for _, endpoint := range []string{eassdk.EndpointTypeDefault, eassdk.EndpointTypeDocker} {
		t.Run(endpoint, func(t *testing.T) {
			var calls atomic.Int32
			started, disconnected := make(chan struct{}), make(chan struct{})
			reply, _ := proto.Marshal(&easyrec.PBResponse{StatusCode: easyrec.StatusCode_OK})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				calls.Add(1)
				if r.Header.Get(requesttrace.BatchIDHeader) != "rank-1" {
					t.Error("batch metadata changed")
				}
				if r.Header.Get(requesttrace.RequestIDHeader) == "canceled" {
					close(started)
					<-r.Context().Done()
					close(disconnected)
					return
				}
				w.Write(reply)
			}))
			defer server.Close()
			m := cancelTestModel(t, server.URL, endpoint, 1000)
			request := &easyrec.PBRequest{ItemIds: []string{"item-a"}}
			caller, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := m.RunWithTrace(request, requesttrace.Trace{RequestID: "canceled", BatchID: "rank-1", Context: caller})
				done <- err
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("request never reached backend")
			}
			// Run a second caller through the same model/client while canceling one.
			cancel()
			if _, err := m.RunWithTrace(request, requesttrace.Trace{RequestID: "active", BatchID: "rank-1", Context: context.Background()}); err != nil {
				t.Fatalf("cancellation affected an active caller: %v", err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("caller cancellation lost: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("model did not return on cancellation")
			}
			select {
			case <-disconnected:
			case <-time.After(2 * time.Second):
				t.Fatal("backend did not observe cancellation")
			}
			if _, err := m.RunWithTrace(request, requesttrace.Trace{Context: caller}); !errors.Is(err, context.Canceled) || calls.Load() != 2 {
				t.Fatalf("pre-canceled call or retry sent work: calls=%d err=%v", calls.Load(), err)
			}
		})
	}
}

func TestTraceEasyRecInternalTimeoutStillRetries(t *testing.T) {
	for _, endpoint := range []string{eassdk.EndpointTypeDefault, eassdk.EndpointTypeDocker} {
		t.Run(endpoint, func(t *testing.T) {
			var calls atomic.Int32
			reply, _ := proto.Marshal(&easyrec.PBResponse{StatusCode: easyrec.StatusCode_OK})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if calls.Add(1) < 3 {
					<-r.Context().Done()
					return
				}
				w.Write(reply)
			}))
			defer server.Close()
			m := cancelTestModel(t, server.URL, endpoint, 100)
			caller, cancel := context.WithCancel(context.Background())
			defer cancel()
			if _, err := m.RunWithTrace(&easyrec.PBRequest{ItemIds: []string{"item-a"}}, requesttrace.Trace{Context: caller}); err != nil || calls.Load() != 3 || caller.Err() != nil {
				t.Fatalf("internal timeout changed retries: calls=%d err=%v", calls.Load(), err)
			}
		})
	}
}
