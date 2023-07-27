// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/elastic/apm-aggregation/aggregationpb"
	"github.com/elastic/apm-aggregation/aggregators/internal/hdrhistogram"
	"github.com/elastic/apm-aggregation/aggregators/internal/timestamppb"
)

type TestCombinedMetricsCfg struct {
	eventsTotal            float64
	youngestEventTimestamp time.Time
}

type TestCombinedMetricsOpt func(cfg TestCombinedMetricsCfg) TestCombinedMetricsCfg

func WithEventsTotal(total float64) TestCombinedMetricsOpt {
	return func(cfg TestCombinedMetricsCfg) TestCombinedMetricsCfg {
		cfg.eventsTotal = total
		return cfg
	}
}

func WithYoungestEventTimestamp(ts time.Time) TestCombinedMetricsOpt {
	return func(cfg TestCombinedMetricsCfg) TestCombinedMetricsCfg {
		cfg.youngestEventTimestamp = ts
		return cfg
	}
}

var defaultTestCombinedMetricsCfg = TestCombinedMetricsCfg{
	eventsTotal:            1,
	youngestEventTimestamp: time.Unix(0, 0).UTC(),
}

type TestTransactionCfg struct {
	duration time.Duration
	count    int
}

type TestTransactionOpt func(TestTransactionCfg) TestTransactionCfg

func WithTransactionDuration(d time.Duration) TestTransactionOpt {
	return func(cfg TestTransactionCfg) TestTransactionCfg {
		cfg.duration = d
		return cfg
	}
}

func WithTransactionCount(c int) TestTransactionOpt {
	return func(cfg TestTransactionCfg) TestTransactionCfg {
		cfg.count = c
		return cfg
	}
}

var defaultTestTransactionCfg = TestTransactionCfg{
	duration: time.Second,
	count:    1,
}

type TestSpanCfg struct {
	duration time.Duration
	count    int
}

type TestSpanOpt func(TestSpanCfg) TestSpanCfg

func WithSpanDuration(d time.Duration) TestSpanOpt {
	return func(cfg TestSpanCfg) TestSpanCfg {
		cfg.duration = d
		return cfg
	}
}

func WithSpanCount(c int) TestSpanOpt {
	return func(cfg TestSpanCfg) TestSpanCfg {
		cfg.count = c
		return cfg
	}
}

var defaultTestSpanCfg = TestSpanCfg{
	duration: time.Nanosecond, // for backward compatibility with previous tests
	count:    1,
}

// TestCombinedMetrics creates combined metrics for testing. The creation logic
// is arranged in a way to allow chained creation and addition of leaf nodes
// to combined metrics.
type TestCombinedMetrics CombinedMetrics

func NewTestCombinedMetrics(opts ...TestCombinedMetricsOpt) *TestCombinedMetrics {
	cfg := defaultTestCombinedMetricsCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}
	var cm CombinedMetrics
	cm.EventsTotal = cfg.eventsTotal
	cm.YoungestEventTimestamp = timestamppb.TimeToPBTimestamp(cfg.youngestEventTimestamp)
	cm.Services = make(map[ServiceAggregationKey]ServiceMetrics)
	return (*TestCombinedMetrics)(&cm)
}

func (tcm *TestCombinedMetrics) GetProto() *aggregationpb.CombinedMetrics {
	cm := (*CombinedMetrics)(tcm)
	cmproto := cm.ToProto()
	return cmproto
}

func (tcm *TestCombinedMetrics) Get() CombinedMetrics {
	cm := (*CombinedMetrics)(tcm)
	return *cm
}

type TestServiceMetrics struct {
	sk       ServiceAggregationKey
	tcm      *TestCombinedMetrics
	overflow bool // indicates if the service has overflowed to global
}

func (tcm *TestCombinedMetrics) AddServiceMetrics(
	sk ServiceAggregationKey,
) *TestServiceMetrics {
	if _, ok := tcm.Services[sk]; !ok {
		tcm.Services[sk] = newServiceMetrics()
	}
	return &TestServiceMetrics{sk: sk, tcm: tcm}
}

func (tcm *TestCombinedMetrics) AddServiceMetricsOverflow(
	sk ServiceAggregationKey,
) *TestServiceMetrics {
	if _, ok := tcm.Services[sk]; ok {
		panic("service already added as non overflow")
	}
	// Does not save to a map, any service instance added to this will
	// automatically be overflowed to the global overflow bucket.
	return &TestServiceMetrics{sk: sk, tcm: tcm, overflow: true}
}

type TestServiceInstanceMetrics struct {
	sik      ServiceInstanceAggregationKey
	tsm      *TestServiceMetrics
	overflow bool // indicates if the service instance has overflowed to global
}

func (tsm *TestServiceMetrics) AddServiceInstanceMetrics(
	sik ServiceInstanceAggregationKey,
) *TestServiceInstanceMetrics {
	svc := tsm.tcm.Services[tsm.sk]
	if _, ok := svc.ServiceInstanceGroups[sik]; !ok {
		svc.ServiceInstanceGroups[sik] = newServiceInstanceMetrics()
	}
	return &TestServiceInstanceMetrics{
		sik: sik,
		tsm: tsm,
	}
}

func (tsm *TestServiceMetrics) AddServiceInstanceMetricsOverflow(
	sik ServiceInstanceAggregationKey,
) *TestServiceInstanceMetrics {
	if !tsm.overflow {
		svc := tsm.tcm.Services[tsm.sk]
		if _, ok := svc.ServiceInstanceGroups[sik]; ok {
			panic("service instance already added as non overflow")
		}
	}
	// All service instance overflows to global bucket.
	hash := Hasher{}.
		Chain(tsm.sk.ToProto()).
		Chain(sik.ToProto()).
		Sum()
	insertHash(&tsm.tcm.OverflowServiceInstancesEstimator, hash)
	// Does not save to a map, children of service instance will automatically
	// overflow to the global overflow bucket.
	return &TestServiceInstanceMetrics{
		sik:      sik,
		tsm:      tsm,
		overflow: true,
	}
}

func (tsim *TestServiceInstanceMetrics) AddTransaction(
	tk TransactionAggregationKey,
	opts ...TestTransactionOpt,
) *TestServiceInstanceMetrics {
	if tsim.overflow {
		panic("cannot add transaction to overflowed service transaction")
	}
	cfg := defaultTestTransactionCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}

	hdr := hdrhistogram.New()
	hdr.RecordDuration(cfg.duration, float64(cfg.count))
	ktm := aggregationpb.KeyedTransactionMetricsFromVTPool()
	ktm.Key = tk.ToProto()
	ktm.Metrics = aggregationpb.TransactionMetricsFromVTPool()
	ktm.Metrics.Histogram = HistogramToProto(hdr)

	svc := tsim.tsm.tcm.Services[tsim.tsm.sk]
	svcIns := svc.ServiceInstanceGroups[tsim.sik]
	if oldKtm, ok := svcIns.TransactionGroups[tk]; ok {
		mergeKeyedTransactionMetrics(oldKtm, ktm)
		ktm = oldKtm
	}
	svcIns.TransactionGroups[tk] = ktm
	return tsim
}

func (tsim *TestServiceInstanceMetrics) AddTransactionOverflow(
	tk TransactionAggregationKey,
	opts ...TestTransactionOpt,
) *TestServiceInstanceMetrics {
	cfg := defaultTestTransactionCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}

	hdr := hdrhistogram.New()
	hdr.RecordDuration(cfg.duration, float64(cfg.count))
	from := aggregationpb.TransactionMetricsFromVTPool()
	from.Histogram = HistogramToProto(hdr)

	hash := Hasher{}.
		Chain(tsim.tsm.sk.ToProto()).
		Chain(tsim.sik.ToProto()).
		Chain(tk.ToProto()).
		Sum()
	if tsim.tsm.overflow {
		// Global overflow
		tsim.tsm.tcm.OverflowServices.OverflowTransaction.Merge(from, hash)
	} else {
		// Per service overflow
		svc := tsim.tsm.tcm.Services[tsim.tsm.sk]
		svc.OverflowGroups.OverflowTransaction.Merge(from, hash)
		tsim.tsm.tcm.Services[tsim.tsm.sk] = svc
	}
	return tsim
}

func (tsim *TestServiceInstanceMetrics) AddServiceTransaction(
	stk ServiceTransactionAggregationKey,
	opts ...TestTransactionOpt,
) *TestServiceInstanceMetrics {
	cfg := defaultTestTransactionCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}

	hdr := hdrhistogram.New()
	hdr.RecordDuration(cfg.duration, float64(cfg.count))
	kstm := aggregationpb.KeyedServiceTransactionMetricsFromVTPool()
	kstm.Key = stk.ToProto()
	kstm.Metrics = aggregationpb.ServiceTransactionMetricsFromVTPool()
	kstm.Metrics.Histogram = HistogramToProto(hdr)
	kstm.Metrics.SuccessCount += float64(cfg.count)

	svc := tsim.tsm.tcm.Services[tsim.tsm.sk]
	svcIns := svc.ServiceInstanceGroups[tsim.sik]
	if oldKstm, ok := svcIns.ServiceTransactionGroups[stk]; ok {
		mergeKeyedServiceTransactionMetrics(oldKstm, kstm)
		kstm = oldKstm
	}
	svcIns.ServiceTransactionGroups[stk] = kstm
	return tsim
}

func (tsim *TestServiceInstanceMetrics) AddServiceTransactionOverflow(
	stk ServiceTransactionAggregationKey,
	opts ...TestTransactionOpt,
) *TestServiceInstanceMetrics {
	cfg := defaultTestTransactionCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}

	hdr := hdrhistogram.New()
	hdr.RecordDuration(cfg.duration, float64(cfg.count))
	from := aggregationpb.ServiceTransactionMetricsFromVTPool()
	from.Histogram = HistogramToProto(hdr)
	from.SuccessCount += float64(cfg.count)

	hash := Hasher{}.
		Chain(tsim.tsm.sk.ToProto()).
		Chain(tsim.sik.ToProto()).
		Chain(stk.ToProto()).
		Sum()
	if tsim.tsm.overflow {
		// Global overflow
		tsim.tsm.tcm.OverflowServices.OverflowServiceTransaction.Merge(from, hash)
	} else {
		// Per service overflow
		svc := tsim.tsm.tcm.Services[tsim.tsm.sk]
		svc.OverflowGroups.OverflowServiceTransaction.Merge(from, hash)
		tsim.tsm.tcm.Services[tsim.tsm.sk] = svc
	}
	return tsim
}

func (tsim *TestServiceInstanceMetrics) AddSpan(
	spk SpanAggregationKey,
	opts ...TestSpanOpt,
) *TestServiceInstanceMetrics {
	cfg := defaultTestSpanCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}

	ksm := aggregationpb.KeyedSpanMetricsFromVTPool()
	ksm.Key = spk.ToProto()
	ksm.Metrics = aggregationpb.SpanMetricsFromVTPool()
	ksm.Metrics.Sum += float64(cfg.duration * time.Duration(cfg.count))
	ksm.Metrics.Count += float64(cfg.count)

	svc := tsim.tsm.tcm.Services[tsim.tsm.sk]
	svcIns := svc.ServiceInstanceGroups[tsim.sik]
	if oldKsm, ok := svcIns.SpanGroups[spk]; ok {
		mergeKeyedSpanMetrics(oldKsm, ksm)
		ksm = oldKsm
	}
	svcIns.SpanGroups[spk] = ksm
	return tsim
}

func (tsim *TestServiceInstanceMetrics) AddSpanOverflow(
	spk SpanAggregationKey,
	opts ...TestSpanOpt,
) *TestServiceInstanceMetrics {
	cfg := defaultTestSpanCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}

	from := aggregationpb.SpanMetricsFromVTPool()
	from.Sum += float64(cfg.duration * time.Duration(cfg.count))
	from.Count += float64(cfg.count)

	hash := Hasher{}.
		Chain(tsim.tsm.sk.ToProto()).
		Chain(tsim.sik.ToProto()).
		Chain(spk.ToProto()).
		Sum()
	if tsim.tsm.overflow {
		// Global overflow
		tsim.tsm.tcm.OverflowServices.OverflowSpan.Merge(from, hash)
	} else {
		// Per service overflow
		svc := tsim.tsm.tcm.Services[tsim.tsm.sk]
		svc.OverflowGroups.OverflowSpan.Merge(from, hash)
		tsim.tsm.tcm.Services[tsim.tsm.sk] = svc
	}
	return tsim
}

func (tsim *TestServiceInstanceMetrics) GetProto() *aggregationpb.CombinedMetrics {
	return tsim.tsm.tcm.GetProto()
}

func (tsim *TestServiceInstanceMetrics) Get() CombinedMetrics {
	return tsim.tsm.tcm.Get()
}

// Set of cmp options to sort combined metrics based on key hash. Hash collisions
// are not considered.
var combinedMetricsSliceSorters = []cmp.Option{
	protocmp.SortRepeated(func(a, b *aggregationpb.KeyedServiceMetrics) bool {
		return Hasher{}.Chain(a.Key).Sum() < Hasher{}.Chain(b.Key).Sum()
	}),
	protocmp.SortRepeated(func(a, b *aggregationpb.KeyedServiceInstanceMetrics) bool {
		return Hasher{}.Chain(a.Key).Sum() < Hasher{}.Chain(b.Key).Sum()
	}),
	protocmp.SortRepeated(func(a, b *aggregationpb.KeyedTransactionMetrics) bool {
		return Hasher{}.Chain(a.Key).Sum() < Hasher{}.Chain(b.Key).Sum()
	}),
	protocmp.SortRepeated(func(a, b *aggregationpb.KeyedServiceTransactionMetrics) bool {
		return Hasher{}.Chain(a.Key).Sum() < Hasher{}.Chain(b.Key).Sum()
	}),
	protocmp.SortRepeated(func(a, b *aggregationpb.KeyedSpanMetrics) bool {
		return Hasher{}.Chain(a.Key).Sum() < Hasher{}.Chain(b.Key).Sum()
	}),
}
