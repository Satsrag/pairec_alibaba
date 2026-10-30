package rank

import (
	"fmt"
	"time"

	"github.com/alibaba/pairec/v2/algorithm"
	"github.com/alibaba/pairec/v2/context"
	"github.com/alibaba/pairec/v2/log"
	"github.com/alibaba/pairec/v2/pkg/requesttrace"
	"github.com/alibaba/pairec/v2/utils"
)

// RunAlgoBatch records one algorithm invocation without changing its payload,
// return value, retries, or timeout for active callers. Both rank services
// share the request's batch counter, so their correlation keys cannot collide.
func RunAlgoBatch(algo string, data IAlgoData, recommend *context.RecommendContext) (interface{}, error) {
	trace := recommend.NewRankBatchTrace()
	start := time.Now()
	var result interface{}
	err := trace.RequestContext().Err()
	if err == nil {
		result, err = algorithm.RunWithTrace(algo, data.GetFeatures(), trace)
	}
	status := "ok"
	if err != nil {
		status = "error"
		if trace.RequestContext().Err() != nil {
			status = "canceled"
		}
	}
	log.Info(fmt.Sprintf("requestId=%s\tmodule=RankBatch\tbatchId=%s\talgo=%s\titems=%d\tcost=%d\tstatus=%s",
		requesttrace.LogValue(trace.RequestID), requesttrace.LogValue(trace.BatchID),
		requesttrace.LogValue(algo), len(data.GetItems()), utils.CostTime(start), status))
	return result, err
}
