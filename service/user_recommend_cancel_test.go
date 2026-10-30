package service

import (
	stdcontext "context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alibaba/pairec/v2/algorithm"
	"github.com/alibaba/pairec/v2/algorithm/eas"
	"github.com/alibaba/pairec/v2/context"
	"github.com/alibaba/pairec/v2/module"
	"github.com/alibaba/pairec/v2/pkg/requesttrace"
	"github.com/alibaba/pairec/v2/recconf"
	"github.com/alibaba/pairec/v2/service/hook"
	"github.com/alibaba/pairec/v2/service/pipeline"
	"github.com/alibaba/pairec/v2/service/recall"
	"github.com/alibaba/pairec/v2/sort"
)

type cancelRecommendParam struct{}

func (cancelRecommendParam) GetParameter(name string) interface{} {
	switch name {
	case "scene":
		return "cancel-recommend-test"
	case "category":
		return "default"
	case "uid":
		return "test-user"
	}
	return nil
}

type cancelRecallFunc func(*module.User, *context.RecommendContext) []*module.Item

func (f cancelRecallFunc) GetCandidateItems(user *module.User, ctx *context.RecommendContext) []*module.Item {
	return f(user, ctx)
}

type cancelRecommendAlgo struct {
	cancel          stdcontext.CancelFunc
	pipelineStarted <-chan struct{}
	canceled        chan<- struct{}
}

func (*cancelRecommendAlgo) Init(*recconf.AlgoConfig) error { return nil }
func (*cancelRecommendAlgo) Run(interface{}) (interface{}, error) {
	return nil, fmt.Errorf("missing request context")
}
func (a *cancelRecommendAlgo) RunWithTrace(_ interface{}, trace requesttrace.Trace) (interface{}, error) {
	<-a.pipelineStarted
	a.cancel()
	close(a.canceled)
	return nil, trace.RequestContext().Err()
}

type cancelCountingSort struct{ calls atomic.Int32 }

func (s *cancelCountingSort) Sort(*sort.SortData) error {
	s.calls.Add(1)
	return nil
}

func TestTraceCanceledRecommendWaitsForPipelineAndSkipsDeliveryWork(t *testing.T) {
	caller, cancel := stdcontext.WithCancel(stdcontext.Background())
	defer cancel()
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	items := func() []*module.Item { return []*module.Item{module.NewItem("item-a")} }
	recall.RegisterRecall("cancel-main", cancelRecallFunc(func(*module.User, *context.RecommendContext) []*module.Item { return items() }))
	recall.RegisterRecall("cancel-pipeline", cancelRecallFunc(func(*module.User, *context.RecommendContext) []*module.Item {
		close(started)
		<-release
		return items()
	}))
	algorithm.RegisterAlgorithm("cancel-recommend-algo", &cancelRecommendAlgo{cancel: cancel, pipelineStarted: started, canceled: canceled})
	countingSort := &cancelCountingSort{}
	sort.RegisterSort("cancel-recommend-sort", countingSort)
	config := &recconf.RecommendConfig{
		SceneConfs: map[string]map[string]recconf.CategoryConfig{"cancel-recommend-test": {"default": {RecallNames: []string{"cancel-main"}}}},
		RankConf: map[string]recconf.RankConfig{"cancel-recommend-test": {
			Processor: eas.Eas_Processor_EASYREC, BatchCount: 1, RankAlgoList: []string{"cancel-recommend-algo"},
		}},
		SortNames: map[string][]string{"cancel-recommend-test": {"cancel-recommend-sort"}},
		PipelineConfs: map[string][]recconf.PipelineConfig{"cancel-recommend-test": {{
			Name: "cancel-pipeline", RecallNames: []string{"cancel-pipeline"}, SortNames: []string{"cancel-recommend-sort"},
		}}},
	}
	savedConfig, savedScenes, savedHooks := recconf.Config, scenes, hook.RecommendCleanHooks
	recconf.Config = config
	scenes = make(map[string]*Scene)
	var hookCalls atomic.Int32
	hook.RecommendCleanHooks = []hook.RecommendCleanHookFunc{func(*context.RecommendContext, ...interface{}) { hookCalls.Add(1) }}
	Load(config)
	sort.Load(config)
	pipeline.LoadPipelineConfigs(config)
	t.Cleanup(func() {
		recconf.Config, scenes, hook.RecommendCleanHooks = savedConfig, savedScenes, savedHooks
		pipeline.LoadPipelineConfigs(savedConfig)
	})
	ctx := context.NewRecommendContext()
	ctx.SetRequestContext(caller)
	ctx.Param, ctx.Config, ctx.Size = cancelRecommendParam{}, config, 1
	ctx.RecommendId = "cancel-recommend"
	done := make(chan []*module.Item, 1)
	go func() { done <- NewUserRecommendService().Recommend(ctx) }()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("main rank did not cancel the caller")
	}
	select {
	case <-done:
		t.Fatal("recommend returned with a pipeline still running")
	default:
	}
	unblock()
	select {
	case got := <-done:
		if got != nil || countingSort.calls.Load() != 0 || hookCalls.Load() != 0 {
			t.Fatal("canceled results reached sort, clean hooks, or delivery")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("recommend did not finish after its pipeline drained")
	}
}
