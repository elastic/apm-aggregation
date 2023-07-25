// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"time"

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
	youngestEventTimestamp: time.Time{},
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
	sk  ServiceAggregationKey
	tcm *TestCombinedMetrics
}

func (tcm *TestCombinedMetrics) AddServiceMetrics(
	sk ServiceAggregationKey,
) *TestServiceMetrics {
	tcm.Services[sk] = newServiceMetrics()
	return &TestServiceMetrics{sk: sk, tcm: tcm}
}

type TestServiceInstanceMetrics struct {
	sk  ServiceAggregationKey
	sik ServiceInstanceAggregationKey
	tcm *TestCombinedMetrics
}

func (tsm *TestServiceMetrics) AddServiceInstanceMetrics(
	sik ServiceInstanceAggregationKey,
) *TestServiceInstanceMetrics {
	svc := tsm.tcm.Services[tsm.sk]
	svc.ServiceInstanceGroups[sik] = newServiceInstanceMetrics()
	return &TestServiceInstanceMetrics{
		sik: sik,
		sk:  tsm.sk,
		tcm: tsm.tcm,
	}
}

func (tsim *TestServiceInstanceMetrics) GetProto() *aggregationpb.CombinedMetrics {
	return tsim.tcm.GetProto()
}

func (tsim *TestServiceInstanceMetrics) Get() CombinedMetrics {
	return tsim.tcm.Get()
}

type TestTransactionCfg struct {
	duration time.Duration
	count    int
}

func (tsim *TestServiceInstanceMetrics) AddGlobalServiceInstanceOverflow(
	sk ServiceAggregationKey,
	sik ServiceInstanceAggregationKey,
) *TestServiceInstanceMetrics {
	hash := Hasher{}.
		Chain(sk.ToProto()).
		Chain(sik.ToProto()).
		Sum()
	insertHash(&tsim.tcm.OverflowServiceInstancesEstimator, hash)
	return tsim
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

func (tsim *TestServiceInstanceMetrics) AddTransaction(
	tk TransactionAggregationKey,
	opts ...TestTransactionOpt,
) *TestServiceInstanceMetrics {
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

	svc := tsim.tcm.Services[tsim.sk]
	svcIns := svc.ServiceInstanceGroups[tsim.sik]
	svcIns.TransactionGroups[tk] = ktm
	return tsim
}

func (tsim *TestServiceInstanceMetrics) AddTransactionOverflow(
	tk TransactionAggregationKey,
	opts ...TestTransactionOpt,
) *TestServiceInstanceMetrics {
	return tsim.AddTransactionOverflowWithServiceInstance(tsim.sik, tk, opts...)
}

func (tsim *TestServiceInstanceMetrics) AddTransactionOverflowWithServiceInstance(
	sik ServiceInstanceAggregationKey,
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

	svc := tsim.tcm.Services[tsim.sk]
	hash := Hasher{}.
		Chain(tsim.sk.ToProto()).
		Chain(sik.ToProto()).
		Chain(tk.ToProto()).
		Sum()
	svc.OverflowGroups.OverflowTransaction.Merge(from, hash)
	tsim.tcm.Services[tsim.sk] = svc
	return tsim
}

func (tsim *TestServiceInstanceMetrics) AddGlobalTransactionOverflow(
	sk ServiceAggregationKey,
	sik ServiceInstanceAggregationKey,
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

	sikHasher := Hasher{}.
		Chain(sk.ToProto()).
		Chain(sik.ToProto())
	hash := sikHasher.
		Chain(tk.ToProto()).
		Sum()
	tsim.tcm.OverflowServices.OverflowTransaction.Merge(from, hash)
	insertHash(&tsim.tcm.OverflowServiceInstancesEstimator, sikHasher.Sum())
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

	svc := tsim.tcm.Services[tsim.sk]
	svcIns := svc.ServiceInstanceGroups[tsim.sik]
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

	svc := tsim.tcm.Services[tsim.sk]
	hash := Hasher{}.
		Chain(tsim.sk.ToProto()).
		Chain(tsim.sik.ToProto()).
		Chain(stk.ToProto()).
		Sum()
	svc.OverflowGroups.OverflowServiceTransaction.Merge(from, hash)
	tsim.tcm.Services[tsim.sk] = svc
	return tsim
}

func (tsim *TestServiceInstanceMetrics) AddGlobalServiceTransactionOverflow(
	sk ServiceAggregationKey,
	sik ServiceInstanceAggregationKey,
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

	sikHasher := Hasher{}.
		Chain(sk.ToProto()).
		Chain(sik.ToProto())
	hash := sikHasher.
		Chain(stk.ToProto()).
		Sum()
	tsim.tcm.OverflowServices.OverflowServiceTransaction.Merge(from, hash)
	insertHash(&tsim.tcm.OverflowServiceInstancesEstimator, sikHasher.Sum())
	return tsim
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

	svc := tsim.tcm.Services[tsim.sk]
	svcIns := svc.ServiceInstanceGroups[tsim.sik]
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

	svc := tsim.tcm.Services[tsim.sk]
	hash := Hasher{}.
		Chain(tsim.sk.ToProto()).
		Chain(tsim.sik.ToProto()).
		Chain(spk.ToProto()).
		Sum()
	svc.OverflowGroups.OverflowSpan.Merge(from, hash)
	tsim.tcm.Services[tsim.sk] = svc
	return tsim
}

func (tsim *TestServiceInstanceMetrics) AddGlobalSpanOverflow(
	sk ServiceAggregationKey,
	sik ServiceInstanceAggregationKey,
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

	sikHasher := Hasher{}.
		Chain(sk.ToProto()).
		Chain(sik.ToProto())
	hash := sikHasher.
		Chain(spk.ToProto()).
		Sum()
	tsim.tcm.OverflowServices.OverflowSpan.Merge(from, hash)
	insertHash(&tsim.tcm.OverflowServiceInstancesEstimator, sikHasher.Sum())
	return tsim
}
