// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/elastic/apm-aggregation/aggregationpb"
	"github.com/elastic/apm-aggregation/aggregators/internal/hdrhistogram"
	"github.com/elastic/apm-aggregation/aggregators/internal/protohash"
	"github.com/elastic/apm-data/model/modelpb"
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
	// outcome is used for service transaction as transaction already
	// have `EventOutcome` in their key. For transactions this field
	// will automatically be overriden based on the key value.
	outcome string
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

// WithEventOutcome is used to specify the event outcome for building
// test service transaction metrics. If it is specified for building
// test transaction metrics then it will be overridden based on the
// `EventOutcome` in the transaction aggregation key.
func WithEventOutcome(o string) TestTransactionOpt {
	return func(cfg TestTransactionCfg) TestTransactionCfg {
		cfg.outcome = o
		return cfg
	}
}

var defaultTestTransactionCfg = TestTransactionCfg{
	duration: time.Second,
	count:    1,
	outcome:  "success",
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
type TestCombinedMetrics combinedMetrics

func NewTestCombinedMetrics(opts ...TestCombinedMetricsOpt) *TestCombinedMetrics {
	cfg := defaultTestCombinedMetricsCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}
	var cm combinedMetrics
	cm.EventsTotal = cfg.eventsTotal
	cm.YoungestEventTimestamp = modelpb.FromTime(cfg.youngestEventTimestamp)
	cm.Services = make(map[serviceAggregationKey]serviceMetrics)
	return (*TestCombinedMetrics)(&cm)
}

func (tcm *TestCombinedMetrics) GetProto() *aggregationpb.CombinedMetrics {
	cm := (*combinedMetrics)(tcm)
	cmproto := cm.ToProto()
	return cmproto
}

func (tcm *TestCombinedMetrics) Get() combinedMetrics {
	cm := (*combinedMetrics)(tcm)
	return *cm
}

type TestServiceMetrics struct {
	sk       serviceAggregationKey
	tcm      *TestCombinedMetrics
	overflow bool // indicates if the service has overflowed to global
}

func (tcm *TestCombinedMetrics) AddServiceMetrics(
	sk serviceAggregationKey,
) *TestServiceMetrics {
	if _, ok := tcm.Services[sk]; !ok {
		tcm.Services[sk] = newServiceMetrics()
	}
	return &TestServiceMetrics{sk: sk, tcm: tcm}
}

func (tcm *TestCombinedMetrics) AddServiceMetricsOverflow(
	sk serviceAggregationKey,
) *TestServiceMetrics {
	if _, ok := tcm.Services[sk]; ok {
		panic("service already added as non overflow")
	}
	// Does not save to a map, any service instance added to this will
	// automatically be overflowed to the global overflow bucket.
	return &TestServiceMetrics{sk: sk, tcm: tcm, overflow: true}
}

type TestServiceInstanceMetrics struct {
	sik      serviceInstanceAggregationKey
	tsm      *TestServiceMetrics
	overflow bool // indicates if the service instance has overflowed to global
}

func (tsm *TestServiceMetrics) AddServiceInstanceMetrics(
	sik serviceInstanceAggregationKey,
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
	sik serviceInstanceAggregationKey,
) *TestServiceInstanceMetrics {
	if !tsm.overflow {
		svc := tsm.tcm.Services[tsm.sk]
		if _, ok := svc.ServiceInstanceGroups[sik]; ok {
			panic("service instance already added as non overflow")
		}
	}
	// All service instance overflows to global bucket.
	hash := protohash.HashServiceInstanceAggregationKey(
		protohash.HashServiceAggregationKey(xxhash.Digest{}, tsm.sk.ToProto()),
		sik.ToProto(),
	)
	insertHash(&tsm.tcm.OverflowServiceInstancesEstimator, hash.Sum64())
	// Does not save to a map, children of service instance will automatically
	// overflow to the global overflow bucket.
	return &TestServiceInstanceMetrics{
		sik:      sik,
		tsm:      tsm,
		overflow: true,
	}
}

func (tsim *TestServiceInstanceMetrics) AddTransaction(
	tk transactionAggregationKey,
	opts ...TestTransactionOpt,
) *TestServiceInstanceMetrics {
	if tsim.overflow {
		panic("cannot add transaction to overflowed service transaction")
	}
	cfg := defaultTestTransactionCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}
	cfg.outcome = tk.EventOutcome

	hdr := hdrhistogram.New()
	hdr.RecordDuration(cfg.duration, float64(cfg.count))
	ktm := aggregationpb.KeyedTransactionMetricsFromVTPool()
	ktm.Key = tk.ToProto()
	ktm.Metrics = aggregationpb.TransactionMetricsFromVTPool()
	ktm.Metrics.Histogram = histogramToProto(hdr)

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
	tk transactionAggregationKey,
	opts ...TestTransactionOpt,
) *TestServiceInstanceMetrics {
	cfg := defaultTestTransactionCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}
	cfg.outcome = tk.EventOutcome

	hdr := hdrhistogram.New()
	hdr.RecordDuration(cfg.duration, float64(cfg.count))
	from := aggregationpb.TransactionMetricsFromVTPool()
	from.Histogram = histogramToProto(hdr)

	hash := protohash.HashTransactionAggregationKey(
		protohash.HashServiceInstanceAggregationKey(
			protohash.HashServiceAggregationKey(xxhash.Digest{}, tsim.tsm.sk.ToProto()),
			tsim.sik.ToProto(),
		),
		tk.ToProto(),
	)
	if tsim.tsm.overflow {
		// Global overflow
		tsim.tsm.tcm.OverflowServices.OverflowTransaction.Merge(from, hash.Sum64())
	} else {
		// Per service overflow
		svc := tsim.tsm.tcm.Services[tsim.tsm.sk]
		svc.OverflowGroups.OverflowTransaction.Merge(from, hash.Sum64())
		tsim.tsm.tcm.Services[tsim.tsm.sk] = svc
	}
	return tsim
}

func (tsim *TestServiceInstanceMetrics) AddServiceTransaction(
	stk serviceTransactionAggregationKey,
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
	kstm.Metrics.Histogram = histogramToProto(hdr)
	switch cfg.outcome {
	case "failure":
		kstm.Metrics.FailureCount = float64(cfg.count)
	case "success":
		kstm.Metrics.SuccessCount = float64(cfg.count)
	}

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
	stk serviceTransactionAggregationKey,
	opts ...TestTransactionOpt,
) *TestServiceInstanceMetrics {
	cfg := defaultTestTransactionCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}

	hdr := hdrhistogram.New()
	hdr.RecordDuration(cfg.duration, float64(cfg.count))
	from := aggregationpb.ServiceTransactionMetricsFromVTPool()
	from.Histogram = histogramToProto(hdr)
	switch cfg.outcome {
	case "failure":
		from.FailureCount = float64(cfg.count)
	case "success":
		from.SuccessCount = float64(cfg.count)
	}

	hash := protohash.HashServiceTransactionAggregationKey(
		protohash.HashServiceInstanceAggregationKey(
			protohash.HashServiceAggregationKey(xxhash.Digest{}, tsim.tsm.sk.ToProto()),
			tsim.sik.ToProto(),
		),
		stk.ToProto(),
	)
	if tsim.tsm.overflow {
		// Global overflow
		tsim.tsm.tcm.OverflowServices.OverflowServiceTransaction.Merge(from, hash.Sum64())
	} else {
		// Per service overflow
		svc := tsim.tsm.tcm.Services[tsim.tsm.sk]
		svc.OverflowGroups.OverflowServiceTransaction.Merge(from, hash.Sum64())
		tsim.tsm.tcm.Services[tsim.tsm.sk] = svc
	}
	return tsim
}

func (tsim *TestServiceInstanceMetrics) AddSpan(
	spk spanAggregationKey,
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
	spk spanAggregationKey,
	opts ...TestSpanOpt,
) *TestServiceInstanceMetrics {
	cfg := defaultTestSpanCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}

	from := aggregationpb.SpanMetricsFromVTPool()
	from.Sum += float64(cfg.duration * time.Duration(cfg.count))
	from.Count += float64(cfg.count)

	hash := protohash.HashSpanAggregationKey(
		protohash.HashServiceInstanceAggregationKey(
			protohash.HashServiceAggregationKey(xxhash.Digest{}, tsim.tsm.sk.ToProto()),
			tsim.sik.ToProto(),
		),
		spk.ToProto(),
	)
	if tsim.tsm.overflow {
		// Global overflow
		tsim.tsm.tcm.OverflowServices.OverflowSpan.Merge(from, hash.Sum64())
	} else {
		// Per service overflow
		svc := tsim.tsm.tcm.Services[tsim.tsm.sk]
		svc.OverflowGroups.OverflowSpan.Merge(from, hash.Sum64())
		tsim.tsm.tcm.Services[tsim.tsm.sk] = svc
	}
	return tsim
}

func (tsim *TestServiceInstanceMetrics) GetProto() *aggregationpb.CombinedMetrics {
	return tsim.tsm.tcm.GetProto()
}

func (tsim *TestServiceInstanceMetrics) Get() combinedMetrics {
	return tsim.tsm.tcm.Get()
}

// Set of cmp options to sort combined metrics based on key hash. Hash collisions
// are not considered.
var combinedMetricsSliceSorters = []cmp.Option{
	protocmp.SortRepeated(func(a, b *aggregationpb.KeyedServiceMetrics) bool {
		return xxhashDigestLess(
			protohash.HashServiceAggregationKey(xxhash.Digest{}, a.Key),
			protohash.HashServiceAggregationKey(xxhash.Digest{}, b.Key),
		)
	}),
	protocmp.SortRepeated(func(a, b *aggregationpb.KeyedServiceInstanceMetrics) bool {
		return xxhashDigestLess(
			protohash.HashServiceInstanceAggregationKey(xxhash.Digest{}, a.Key),
			protohash.HashServiceInstanceAggregationKey(xxhash.Digest{}, b.Key),
		)
	}),
	protocmp.SortRepeated(func(a, b *aggregationpb.KeyedTransactionMetrics) bool {
		return xxhashDigestLess(
			protohash.HashTransactionAggregationKey(xxhash.Digest{}, a.Key),
			protohash.HashTransactionAggregationKey(xxhash.Digest{}, b.Key),
		)
	}),
	protocmp.SortRepeated(func(a, b *aggregationpb.KeyedServiceTransactionMetrics) bool {
		return xxhashDigestLess(
			protohash.HashServiceTransactionAggregationKey(xxhash.Digest{}, a.Key),
			protohash.HashServiceTransactionAggregationKey(xxhash.Digest{}, b.Key),
		)
	}),
	protocmp.SortRepeated(func(a, b *aggregationpb.KeyedSpanMetrics) bool {
		return xxhashDigestLess(
			protohash.HashSpanAggregationKey(xxhash.Digest{}, a.Key),
			protohash.HashSpanAggregationKey(xxhash.Digest{}, b.Key),
		)
	}),
}

func xxhashDigestLess(a, b xxhash.Digest) bool {
	return a.Sum64() < b.Sum64()
}
