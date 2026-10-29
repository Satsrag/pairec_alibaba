package requesttrace

import (
	"net/http"
	"strings"
	"testing"
)

func TestTraceIDsAndHeaders(t *testing.T) {
	for _, value := range []string{"request-1", "rank_1:abc.def", strings.Repeat("a", 128)} {
		if !ValidID(value) || LogValue(value) != value {
			t.Errorf("valid identifier rejected: %q", value)
		}
	}
	for _, value := range []string{"", "a b", "a\tb", "a\r\nInjected:yes", "用户", "a/b", strings.Repeat("a", 129)} {
		if ValidID(value) || LogValue(value) != "-" {
			t.Errorf("invalid identifier accepted: %q", value)
		}
		h := http.Header{"Authorization": {"test-token"}}
		h.Set(RequestIDHeader, "static-request")
		h.Set(BatchIDHeader, "static-batch")
		(Trace{RequestID: value, BatchID: value}).SetHeaders(h)
		if h.Get(RequestIDHeader) != "" || h.Get(BatchIDHeader) != "" || h.Get("Authorization") != "test-token" {
			t.Fatalf("invalid trace leaked or changed authentication: %v", h)
		}
	}
	h := make(http.Header)
	(Trace{RequestID: "request-1", BatchID: "rank-1"}).SetHeaders(h)
	if h.Get(RequestIDHeader) != "request-1" || h.Get(BatchIDHeader) != "rank-1" {
		t.Fatal("valid trace headers missing")
	}
}
