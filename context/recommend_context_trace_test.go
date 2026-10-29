package context

import (
	"sync"
	"testing"

	"github.com/alibaba/pairec/v2/pkg/requesttrace"
)

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
