package rank_test

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/alibaba/pairec/v2/algorithm"
	"github.com/alibaba/pairec/v2/algorithm/eas"
	"github.com/alibaba/pairec/v2/algorithm/eas/easyrec"
	"github.com/alibaba/pairec/v2/algorithm/response"
	"github.com/alibaba/pairec/v2/context"
	"github.com/alibaba/pairec/v2/module"
	"github.com/alibaba/pairec/v2/pkg/requesttrace"
	"github.com/alibaba/pairec/v2/recconf"
	"github.com/alibaba/pairec/v2/service/pipeline"
	"github.com/alibaba/pairec/v2/service/rank"
)

type traceRankParam struct{}

func (traceRankParam) GetParameter(name string) interface{} {
	if name == "scene" {
		return "trace-test"
	}
	return nil
}

type traceRankResponse struct{ score float64 }

func (r traceRankResponse) GetScore() float64 { return r.score }
func (r traceRankResponse) GetScoreMap() map[string]float64 {
	return map[string]float64{"ctr": r.score, "finish": 2 * r.score}
}
func (traceRankResponse) GetModuleType() bool { return true }

type rankTraceCall struct {
	trace requesttrace.Trace
	items []string
}

type traceRankAlgo struct {
	mu    sync.Mutex
	calls []rankTraceCall
}

func (*traceRankAlgo) Init(*recconf.AlgoConfig) error { return nil }
func (*traceRankAlgo) Run(interface{}) (interface{}, error) {
	return nil, fmt.Errorf("rank did not use the traced invocation")
}
func (a *traceRankAlgo) RunWithTrace(data interface{}, trace requesttrace.Trace) (interface{}, error) {
	req, ok := data.(*easyrec.PBRequest)
	if !ok {
		return nil, fmt.Errorf("unexpected algorithm data %T", data)
	}
	if len(req.UserFeatures) != 1 || req.UserFeatures["test_feature"] == nil {
		return nil, fmt.Errorf("trace changed the model feature set")
	}
	result := make([]response.AlgoResponse, len(req.ItemIds))
	for i, id := range req.ItemIds {
		n, err := strconv.ParseFloat(id, 64)
		if err != nil {
			return nil, err
		}
		result[i] = traceRankResponse{score: n / 10}
	}
	a.mu.Lock()
	a.calls = append(a.calls, rankTraceCall{trace: trace, items: append([]string(nil), req.ItemIds...)})
	a.mu.Unlock()
	return result, nil
}

func TestTraceRankBatchesKeepScoresAndOrder(t *testing.T) {
	const algoName = "trace_rank_test"
	algo := &traceRankAlgo{}
	algorithm.RegisterAlgorithm(algoName, algo)
	algorithm.RegisterAlgorithm("trace_rank_extra", algo)
	saved := recconf.Config
	config := recconf.RankConfig{
		Processor: eas.Eas_Processor_EASYREC, BatchCount: 2,
		RankAlgoList: []string{algoName, "trace_rank_extra"}, RankScore: "${" + algoName + "_ctr}",
	}
	recconf.Config = &recconf.RecommendConfig{RankConf: map[string]recconf.RankConfig{"trace-test": config}}
	t.Cleanup(func() { recconf.Config = saved })
	ctx := context.NewRecommendContext()
	ctx.RecommendId = "request-a"
	ctx.Param = traceRankParam{}
	user := module.NewUser("test-user")
	user.AddProperty("test_feature", "unchanged")
	items := []*module.Item{module.NewItem("5"), module.NewItem("2"), module.NewItem("4"), module.NewItem("1"), module.NewItem("3")}
	ids := make([]module.ItemId, len(items))
	for i, item := range items {
		ids[i] = item.Id
	}
	check := func() {
		t.Helper()
		for i, item := range items {
			n, _ := strconv.ParseFloat(string(item.Id), 64)
			if item.Id != ids[i] || item.Score != n/10 || item.GetAlgoScore(algoName+"_finish") != 2*n/10 {
				t.Fatalf("score/head/order changed at %d: id=%s score=%v", i, item.Id, item.Score)
			}
		}
	}
	rank.NewRankService().Rank(user, items, ctx)
	check()
	pipeline.NewRankService(&recconf.PipelineConfig{Name: "trace-pipeline", RankConf: config}).Rank(user, items, ctx)
	check()
	seen := make(map[string]bool)
	counts := map[int]int{}
	for _, call := range algo.calls {
		if call.trace.RequestID != ctx.RecommendId || !requesttrace.ValidID(call.trace.BatchID) || seen[call.trace.BatchID] {
			t.Fatalf("invalid or reused batch identity: %+v", call.trace)
		}
		seen[call.trace.BatchID] = true
		counts[len(call.items)]++
	}
	if len(seen) != 12 || !reflect.DeepEqual(counts, map[int]int{1: 4, 2: 8}) {
		t.Fatalf("batch boundaries changed: calls=%d sizes=%v", len(seen), counts)
	}
}
