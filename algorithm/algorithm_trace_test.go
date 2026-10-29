package algorithm

import (
	"testing"

	"github.com/alibaba/pairec/v2/pkg/requesttrace"
	"github.com/alibaba/pairec/v2/recconf"
)

type legacyTraceTestAlgo struct{ calls int }

func (*legacyTraceTestAlgo) Init(*recconf.AlgoConfig) error { return nil }
func (a *legacyTraceTestAlgo) Run(data interface{}) (interface{}, error) {
	a.calls++
	return data, nil
}

type tracedTestAlgo struct {
	legacyTraceTestAlgo
	trace requesttrace.Trace
}

func (a *tracedTestAlgo) RunWithTrace(data interface{}, trace requesttrace.Trace) (interface{}, error) {
	a.trace = trace
	return data, nil
}

func TestTraceFactoryCompatibility(t *testing.T) {
	factory := NewAlgorithmFactory()
	legacy := &legacyTraceTestAlgo{}
	traced := &tracedTestAlgo{}
	factory.algorithms["legacy"] = legacy
	factory.algorithms["traced"] = traced
	transforms := 0
	factory.requestDataFuncs["traced"] = func(_ string, data interface{}) interface{} {
		transforms++
		return data.(string) + "-transformed"
	}
	trace := requesttrace.Trace{RequestID: "request-a", BatchID: "rank-1"}
	if result, err := factory.RunWithTrace("legacy", "payload", trace); err != nil || result != "payload" || legacy.calls != 1 {
		t.Fatal("legacy algorithm fallback changed")
	}
	if result, err := factory.RunWithTrace("traced", "payload", trace); err != nil || result != "payload-transformed" || traced.trace != trace || transforms != 1 || traced.calls != 0 {
		t.Fatal("trace dispatch or data transformation changed")
	}
	if result, err := factory.Run("traced", "legacy"); err != nil || result != "legacy-transformed" || transforms != 2 || traced.calls != 1 {
		t.Fatal("legacy Run must still invoke Run, not the optional trace API")
	}
	if _, err := factory.RunWithTrace("missing", nil, trace); err == nil {
		t.Fatal("missing algorithm must retain its error")
	}
}
