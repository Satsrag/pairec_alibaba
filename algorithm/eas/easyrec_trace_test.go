package eas

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/alibaba/pairec/v2/algorithm/eas/easyrec"
	"github.com/alibaba/pairec/v2/algorithm/response"
	eassdk "github.com/alibaba/pairec/v2/pkg/eas"
	"github.com/alibaba/pairec/v2/pkg/requesttrace"
	"github.com/alibaba/pairec/v2/recconf"
	"github.com/golang/protobuf/proto"
)

func TestTraceEasyRecModelHTTPParity(t *testing.T) {
	request := &easyrec.PBRequest{
		ItemIds: []string{"item-b", "item-a"},
		UserFeatures: map[string]*easyrec.PBFeature{
			"test_feature": {Value: &easyrec.PBFeature_StringFeature{StringFeature: "unchanged"}},
		},
	}
	original, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := proto.Marshal(&easyrec.PBResponse{
		StatusCode: easyrec.StatusCode_OK,
		Outputs:    []string{"ctr", "finish"},
		Results: map[string]*easyrec.Results{
			"item-a": {Scores: []float64{0.2, 0.6}},
			"item-b": {Scores: []float64{0.3, 0.9}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	headers := make(chan http.Header, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !bytes.Equal(body, original) {
			t.Error("trace changed the serialized protobuf payload")
		}
		if r.Header.Get("Authorization") != "test-token" {
			t.Error("trace changed authorization")
		}
		headers <- r.Header.Clone()
		w.Write(reply)
	}))
	defer server.Close()
	for _, endpointType := range []string{eassdk.EndpointTypeDefault, eassdk.EndpointTypeDocker} {
		t.Run(endpointType, func(t *testing.T) {
			model := NewEasModel("rank-test")
			if err := model.Init(&recconf.AlgoConfig{EasConf: recconf.EasConfig{
				Processor: Eas_Processor_EASYREC, EndpointType: endpointType,
				Url: server.URL + "/api/predict/rank-test", Auth: "test-token", Timeout: 1000,
				RetryTimes: 1, ResponseFuncName: "easyrecMutValResponseFunc",
			}}); err != nil {
				t.Fatal(err)
			}
			legacy, err := model.Run(request)
			if err != nil {
				t.Fatal(err)
			}
			legacyHeaders := <-headers
			if legacyHeaders.Get(requesttrace.RequestIDHeader) != "" || legacyHeaders.Get(requesttrace.BatchIDHeader) != "" {
				t.Fatal("legacy model request unexpectedly acquired trace metadata")
			}
			caller, cancel := context.WithCancel(context.Background())
			defer cancel()
			traced, err := model.RunWithTrace(request, requesttrace.Trace{RequestID: "request-a", BatchID: "rank-1", Context: caller})
			if err != nil {
				t.Fatal(err)
			}
			h := <-headers
			if h.Get(requesttrace.RequestIDHeader) != "request-a" || h.Get(requesttrace.BatchIDHeader) != "rank-1" {
				t.Fatal("trace did not reach the actual EasyRec HTTP path")
			}
			before, after := legacy.([]response.AlgoResponse), traced.([]response.AlgoResponse)
			if len(before) != 2 || len(after) != 2 {
				t.Fatal("item count changed")
			}
			for i := range before {
				if !reflect.DeepEqual(before[i].GetScoreMap(), after[i].GetScoreMap()) {
					t.Fatal("trace changed output heads or item ordering")
				}
			}
			if after[0].GetScoreMap()["ctr"] != 0.3 || after[1].GetScoreMap()["finish"] != 0.6 {
				t.Fatal("response no longer follows request item order")
			}
			current, _ := proto.Marshal(request)
			if !bytes.Equal(current, original) {
				t.Fatal("model invocation mutated request protobuf")
			}
		})
	}
}

func TestTraceEasyRecRetryStability(t *testing.T) {
	for _, endpointType := range []string{eassdk.EndpointTypeDefault, eassdk.EndpointTypeDocker} {
		t.Run(endpointType, func(t *testing.T) {
			request := &easyrec.PBRequest{ItemIds: []string{"item-a"}}
			original, _ := proto.Marshal(request)
			reply, _ := proto.Marshal(&easyrec.PBResponse{StatusCode: easyrec.StatusCode_OK})
			headers := make(chan http.Header, 2)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if !bytes.Equal(body, original) {
					t.Error("retry changed serialized protobuf")
				}
				headers <- r.Header.Clone()
				if calls.Add(1) == 1 {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte{255}) // SDK retries status; Docker retries invalid protobuf.
					return
				}
				w.Write(reply)
			}))
			defer server.Close()
			model := NewEasModel("rank-test")
			if err := model.Init(&recconf.AlgoConfig{EasConf: recconf.EasConfig{
				Processor: Eas_Processor_EASYREC, EndpointType: endpointType,
				Url: server.URL + "/api/predict/rank-test", Auth: "test-token", Timeout: 1000, RetryTimes: 2,
			}}); err != nil {
				t.Fatal(err)
			}
			if _, err := model.RunWithTrace(request, requesttrace.Trace{RequestID: "request-retry", BatchID: "rank-7"}); err != nil || calls.Load() != 2 {
				t.Fatalf("configured attempts changed: calls=%d err=%v", calls.Load(), err)
			}
			for i := 0; i < 2; i++ {
				h := <-headers
				if h.Get(requesttrace.RequestIDHeader) != "request-retry" || h.Get(requesttrace.BatchIDHeader) != "rank-7" {
					t.Fatal("logical batch identity changed across attempts")
				}
			}
		})
	}
}
