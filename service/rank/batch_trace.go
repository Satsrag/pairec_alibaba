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
// return value, retries, or timeout. Both standard and pipeline rank services
// share the request's batch counter, so their correlation keys cannot collide.
func RunAlgoBatch(algo string, data IAlgoData, recommend *context.RecommendContext) (interface{}, error) {
	trace := recommend.NewRankBatchTrace()
	start := time.Now()
	result, err := algorithm.RunWithTrace(algo, data.GetFeatures(), trace)
	status := "ok"
	if err != nil {
		status = "error"
	}
	log.Info(fmt.Sprintf("requestId=%s\tmodule=RankBatch\tbatchId=%s\talgo=%s\titems=%d\tcost=%d\tstatus=%s",
		requesttrace.LogValue(trace.RequestID), requesttrace.LogValue(trace.BatchID),
		requesttrace.LogValue(algo), len(data.GetItems()), utils.CostTime(start), status))
	return result, err
}
