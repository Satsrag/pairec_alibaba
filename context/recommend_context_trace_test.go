package context

import (
	stdcontext "context"
	"sync"
	"testing"

	"github.com/alibaba/pairec/v2/pkg/requesttrace"
)

func TestRankBatchTraceCallerContext(t *testing.T) {
	r := NewRecommendContext()
	if r.RequestContext().Err() != nil || r.NewRankBatchTrace().RequestContext().Done() != nil {
		t.Fatal("legacy context unexpectedly has a cancellation source")
	}
	caller, cancel := stdcontext.WithCancel(stdcontext.Background())
	r.SetRequestContext(caller)
	trace := r.NewRankBatchTrace()
	if r.RequestContext() != caller || trace.Context != caller {
		t.Fatal("caller context was not propagated to the batch")
	}
	cancel()
	if trace.RequestContext().Err() != stdcontext.Canceled {
		t.Fatal("batch did not observe caller cancellation")
	}
	r.SetRequestContext(nil)
	if r.RequestContext().Err() != nil {
		t.Fatal("nil did not restore legacy behavior")
	}
}

func TestRankBatchTraceConcurrentIDs(t *testing.T) {
	r := NewRecommendContext()
	r.RecommendId = "request-a"
	const count = 200
	ids := make(chan string, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			trace := r.NewRankBatchTrace()
			if trace.RequestID != "request-a" || !requesttrace.ValidID(trace.BatchID) {
				t.Errorf("invalid batch trace: %+v", trace)
			}
			ids <- trace.BatchID
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[string]bool, count)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate batch ID: %s", id)
		}
		seen[id] = true
	}
	if len(seen) != count || NewRecommendContext().NewRankBatchTrace().BatchID != "rank-1" {
		t.Fatal("batch IDs must be unique within, and scoped to, one recommendation")
	}
}
