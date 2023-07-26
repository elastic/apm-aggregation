// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"io"

	"github.com/axiomhq/hyperloglog"

	"github.com/elastic/apm-aggregation/aggregationpb"
)

type combinedMetricsMerger struct {
	limits  Limits
	metrics CombinedMetrics
}

func (m *combinedMetricsMerger) MergeNewer(value []byte) error {
	from := aggregationpb.CombinedMetricsFromVTPool()
	defer from.ReturnToVTPool()
	if err := from.UnmarshalVT(value); err != nil {
		return err
	}
	m.merge(from)
	return nil
}

func (m *combinedMetricsMerger) MergeOlder(value []byte) error {
	from := aggregationpb.CombinedMetricsFromVTPool()
	defer from.ReturnToVTPool()
	if err := from.UnmarshalVT(value); err != nil {
		return err
	}
	m.merge(from)
	return nil
}

func (m *combinedMetricsMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	data, err := m.metrics.MarshalBinary()
	return data, nil, err
}

func (m *combinedMetricsMerger) merge(from *aggregationpb.CombinedMetrics) {
	// We merge the below fields irrespective of the services present
	// because it is possible for services to be empty if the event
	// does not fit the criteria for aggregations.
	m.metrics.EventsTotal += from.EventsTotal
	if m.metrics.YoungestEventTimestamp < from.YoungestEventTimestamp {
		m.metrics.YoungestEventTimestamp = from.YoungestEventTimestamp
	}
	// If there is overflow due to max services in either of the buckets being
	// merged then we can merge the overflow buckets without considering any other scenarios.
	if from.OverflowServiceInstancesEstimator != nil {
		mergeOverflow(&m.metrics.OverflowServices, from.OverflowServices)
		mergeEstimator(
			&m.metrics.OverflowServiceInstancesEstimator,
			hllSketch(from.OverflowServiceInstancesEstimator),
		)
	}

	if len(from.ServiceMetrics) == 0 {
		return
	}

	// Calculate the current capacity of the transaction, service transaction,
	// and span groups in the _to_ combined metrics.
	totalTransactionGroupsConstraint := newConstraint(0, m.limits.MaxTransactionGroups)
	totalServiceTransactionGroupsConstraint := newConstraint(0, m.limits.MaxServiceTransactionGroups)
	totalSpanGroupsConstraint := newConstraint(0, m.limits.MaxSpanGroups)
	for _, svc := range m.metrics.Services {
		for _, si := range svc.ServiceInstanceGroups {
			totalTransactionGroupsConstraint.add(len(si.TransactionGroups))
			totalServiceTransactionGroupsConstraint.add(len(si.ServiceTransactionGroups))
			totalSpanGroupsConstraint.add(len(si.SpanGroups))
		}
	}

	// Iterate over the services in the _from_ combined metrics and merge them
	// into the _to_ combined metrics as per the following rules:
	// 1. If the service in the _from_ bucket is also present in the _to_
	//    bucket then merge them.
	// 2. If the service in the _from_ bucket is not in the _to_ bucket:
	//    2.a. If the _to_ bucket hasn't breached the max services limit then
	//         create a new service in _to_ bucket and merge.
	//    2.b. Else, merge the _from_ bucket to the overflow service bucket
	//         of the _to_ combined metrics.
	for i := range from.ServiceMetrics {
		fromSvc := from.ServiceMetrics[i]
		hash := Hasher{}.Chain(fromSvc.Key)
		var sk ServiceAggregationKey
		sk.FromProto(fromSvc.Key)
		toSvc, svcOverflow := getServiceMetrics(&m.metrics, sk, m.limits.MaxServices)
		if svcOverflow {
			mergeOverflow(&m.metrics.OverflowServices, fromSvc.Metrics.OverflowGroups)
			for j := range fromSvc.Metrics.ServiceInstanceMetrics {
				ksim := fromSvc.Metrics.ServiceInstanceMetrics[j]
				sikHash := hash.Chain(ksim.Key)
				mergeToOverflowFromSIM(&m.metrics.OverflowServices, ksim, sikHash)
				insertHash(&m.metrics.OverflowServiceInstancesEstimator, sikHash.Sum())
			}
			continue
		}
		if fromSvc.Metrics != nil {
			mergeOverflow(&toSvc.OverflowGroups, fromSvc.Metrics.OverflowGroups)
			mergeServiceInstanceGroups(
				&toSvc,
				fromSvc.Metrics.ServiceInstanceMetrics,
				totalTransactionGroupsConstraint,
				totalServiceTransactionGroupsConstraint,
				totalSpanGroupsConstraint,
				m.limits,
				hash,
				&m.metrics.OverflowServiceInstancesEstimator,
			)
		}
		m.metrics.Services[sk] = toSvc
	}
}

func mergeServiceInstanceGroups(
	to *ServiceMetrics,
	from []*aggregationpb.KeyedServiceInstanceMetrics,
	totalTransactionGroupsConstraint, totalServiceTransactionGroupsConstraint, totalSpanGroupsConstraint *constraint,
	limits Limits,
	hash Hasher,
	overflowServiceInstancesEstimator **hyperloglog.Sketch,
) {
	for i := range from {
		fromSvcIns := from[i]
		var sik ServiceInstanceAggregationKey
		sik.FromProto(fromSvcIns.Key)
		sikHash := hash.Chain(fromSvcIns.Key)

		toSvcIns, overflowed := getServiceInstanceMetrics(to, sik, limits.MaxServiceInstanceGroupsPerService)
		if overflowed {
			mergeToOverflowFromSIM(
				&to.OverflowGroups,
				fromSvcIns,
				sikHash,
			)
			insertHash(
				overflowServiceInstancesEstimator,
				sikHash.Sum(),
			)
			continue
		}
		mergeTransactionGroups(
			toSvcIns.TransactionGroups,
			fromSvcIns.Metrics.TransactionMetrics,
			newConstraint(
				len(toSvcIns.TransactionGroups),
				limits.MaxTransactionGroupsPerService,
			),
			totalTransactionGroupsConstraint,
			hash,
			&to.OverflowGroups.OverflowTransaction,
		)
		mergeServiceTransactionGroups(
			toSvcIns.ServiceTransactionGroups,
			fromSvcIns.Metrics.ServiceTransactionMetrics,
			newConstraint(
				len(toSvcIns.ServiceTransactionGroups),
				limits.MaxServiceTransactionGroupsPerService,
			),
			totalServiceTransactionGroupsConstraint,
			hash,
			&to.OverflowGroups.OverflowServiceTransaction,
		)
		mergeSpanGroups(
			toSvcIns.SpanGroups,
			fromSvcIns.Metrics.SpanMetrics,
			newConstraint(
				len(toSvcIns.SpanGroups),
				limits.MaxSpanGroupsPerService,
			),
			totalSpanGroupsConstraint,
			hash,
			&to.OverflowGroups.OverflowSpan,
		)
		to.ServiceInstanceGroups[sik] = toSvcIns
	}
}

// mergeTransactionGroups merges transaction aggregation groups for two combined metrics
// considering max transaction groups and max transaction groups per service limits.
func mergeTransactionGroups(
	to map[TransactionAggregationKey]*aggregationpb.KeyedTransactionMetrics,
	from []*aggregationpb.KeyedTransactionMetrics,
	perSvcConstraint, globalConstraint *constraint,
	hash Hasher,
	overflowTo *OverflowTransaction,
) {
	for i := range from {
		fromTxn := from[i]
		var tk TransactionAggregationKey
		tk.FromProto(fromTxn.Key)
		toTxn, ok := to[tk]
		if !ok {
			overflowed := perSvcConstraint.maxed() || globalConstraint.maxed()
			if overflowed {
				overflowTo.Merge(
					fromTxn.Metrics,
					hash.Chain(fromTxn.Key).Sum(),
				)
				continue
			}
			perSvcConstraint.add(1)
			globalConstraint.add(1)

			to[tk] = fromTxn
			from[i] = nil
			continue
		}
		mergeKeyedTransactionMetrics(toTxn, fromTxn)
	}
}

// mergeServiceTransactionGroups merges service transaction aggregation groups for two
// combined metrics considering max service transaction groups and max service
// transaction groups per service limits.
func mergeServiceTransactionGroups(
	to map[ServiceTransactionAggregationKey]*aggregationpb.KeyedServiceTransactionMetrics,
	from []*aggregationpb.KeyedServiceTransactionMetrics,
	perSvcConstraint, globalConstraint *constraint,
	hash Hasher,
	overflowTo *OverflowServiceTransaction,
) {
	for i := range from {
		fromSvcTxn := from[i]
		var stk ServiceTransactionAggregationKey
		stk.FromProto(fromSvcTxn.Key)
		toSvcTxn, ok := to[stk]
		if !ok {
			overflowed := perSvcConstraint.maxed() || globalConstraint.maxed()
			if overflowed {
				overflowTo.Merge(
					fromSvcTxn.Metrics,
					hash.Chain(fromSvcTxn.Key).Sum(),
				)
				continue
			}
			perSvcConstraint.add(1)
			globalConstraint.add(1)

			to[stk] = fromSvcTxn
			from[i] = nil
			continue
		}
		mergeKeyedServiceTransactionMetrics(toSvcTxn, fromSvcTxn)
	}
}

// mergeSpanGroups merges span aggregation groups for two combined metrics considering
// max span groups and max span groups per service limits.
func mergeSpanGroups(
	to map[SpanAggregationKey]*aggregationpb.KeyedSpanMetrics,
	from []*aggregationpb.KeyedSpanMetrics,
	perSvcConstraint, globalConstraint *constraint,
	hash Hasher,
	overflowTo *OverflowSpan,
) {
	for i := range from {
		fromSpan := from[i]
		var spk SpanAggregationKey
		spk.FromProto(fromSpan.Key)
		toSpan, ok := to[spk]
		if !ok {
			// Protect against agents that send high cardinality span names by dropping
			// span.name if more than half of the per svc span group limit is reached.
			half := perSvcConstraint.limit / 2
			if perSvcConstraint.value() >= half {
				spk.SpanName = ""
				fromSpan.Key.SpanName = ""
				toSpan, ok = to[spk]
			}
			if !ok {
				overflowed := perSvcConstraint.maxed() || globalConstraint.maxed()
				if overflowed {
					overflowTo.Merge(
						fromSpan.Metrics,
						hash.Chain(fromSpan.Key).Sum(),
					)
					continue
				}
				perSvcConstraint.add(1)
				globalConstraint.add(1)

				to[spk] = fromSpan
				from[i] = nil
				continue
			}
		}
		mergeKeyedSpanMetrics(toSpan, fromSpan)
	}
}

func mergeToOverflowFromSIM(
	to *Overflow,
	from *aggregationpb.KeyedServiceInstanceMetrics,
	hash Hasher,
) {
	if from.Metrics == nil {
		return
	}
	for _, ktm := range from.Metrics.TransactionMetrics {
		to.OverflowTransaction.Merge(
			ktm.Metrics,
			hash.Chain(ktm.Key).Sum(),
		)
	}
	for _, kstm := range from.Metrics.ServiceTransactionMetrics {
		to.OverflowServiceTransaction.Merge(
			kstm.Metrics,
			hash.Chain(kstm.Key).Sum(),
		)
	}
	for _, ksm := range from.Metrics.SpanMetrics {
		to.OverflowSpan.Merge(
			ksm.Metrics,
			hash.Chain(ksm.Key).Sum(),
		)
	}
}

func mergeOverflow(
	to *Overflow,
	fromproto *aggregationpb.Overflow,
) {
	if fromproto == nil {
		return
	}
	var from Overflow
	from.FromProto(fromproto)
	to.OverflowTransaction.MergeOverflow(&from.OverflowTransaction)
	to.OverflowServiceTransaction.MergeOverflow(&from.OverflowServiceTransaction)
	to.OverflowSpan.MergeOverflow(&from.OverflowSpan)
}

func mergeKeyedTransactionMetrics(
	to, from *aggregationpb.KeyedTransactionMetrics,
) {
	if from.Metrics == nil {
		return
	}
	if to.Metrics == nil {
		to.Metrics = aggregationpb.TransactionMetricsFromVTPool()
	}
	mergeTransactionMetrics(to.Metrics, from.Metrics)
}

func mergeTransactionMetrics(
	to, from *aggregationpb.TransactionMetrics,
) {
	if to.Histogram == nil && from.Histogram != nil {
		to.Histogram = aggregationpb.HDRHistogramFromVTPool()
	}
	if to.Histogram != nil && from.Histogram != nil {
		mergeHistogram(to.Histogram, from.Histogram)
	}
}

func mergeKeyedServiceTransactionMetrics(
	to, from *aggregationpb.KeyedServiceTransactionMetrics,
) {
	if from.Metrics == nil {
		return
	}
	if to.Metrics == nil {
		to.Metrics = aggregationpb.ServiceTransactionMetricsFromVTPool()
	}
	mergeServiceTransactionMetrics(to.Metrics, from.Metrics)
}

func mergeServiceTransactionMetrics(
	to, from *aggregationpb.ServiceTransactionMetrics,
) {
	if to.Histogram == nil && from.Histogram != nil {
		to.Histogram = aggregationpb.HDRHistogramFromVTPool()
	}
	if to.Histogram != nil && from.Histogram != nil {
		mergeHistogram(to.Histogram, from.Histogram)
	}
	to.FailureCount += from.FailureCount
	to.SuccessCount += from.SuccessCount
}

func mergeKeyedSpanMetrics(to, from *aggregationpb.KeyedSpanMetrics) {
	if from.Metrics == nil {
		return
	}
	if to.Metrics == nil {
		to.Metrics = aggregationpb.SpanMetricsFromVTPool()
	}
	mergeSpanMetrics(to.Metrics, from.Metrics)
}

func mergeSpanMetrics(to, from *aggregationpb.SpanMetrics) {
	to.Count += from.Count
	to.Sum += from.Sum
}

// TODO: Add tests for merge histograms
func mergeHistogram(to, from *aggregationpb.HDRHistogram) {
	// Assume both histograms are created with same arguments
	m := make(map[int32]int64)
	for i := 0; i < len(to.Buckets); i++ {
		m[to.Buckets[i]] = to.Counts[i]
	}
	for i := 0; i < len(from.Buckets); i++ {
		m[from.Buckets[i]] += from.Counts[i]
	}

	if cap(to.Buckets) < len(m) {
		to.Buckets = make([]int32, len(m))
	}
	if cap(to.Counts) < len(m) {
		to.Counts = make([]int64, len(m))
	}

	to.Buckets = to.Buckets[:0]
	to.Counts = to.Counts[:0]

	for b, c := range m {
		to.Buckets = append(to.Buckets, b)
		to.Counts = append(to.Counts, c)
	}
}

// getServiceMetrics returns the service metric from a combined metrics based on the
// service key argument, creating one if needed. A second bool return value indicates
// if a service is returned or no service can be created due to max svcs limit breach.
func getServiceMetrics(cm *CombinedMetrics, svcKey ServiceAggregationKey, maxSvcs int) (ServiceMetrics, bool) {
	srcSvc, ok := cm.Services[svcKey]
	if !ok {
		if len(cm.Services) < maxSvcs {
			return newServiceMetrics(), false
		}
		return ServiceMetrics{}, true
	}
	return srcSvc, false
}

// getServiceInstanceMetrics returns the service instance metric from a service metrics
// based on the service instance key argument, creating one if needed. A second bool
// return value indicates if a service instance is returned or no service instance can
// be created due to service instance per service limit breach.
func getServiceInstanceMetrics(sm *ServiceMetrics, siKey ServiceInstanceAggregationKey, maxSvcInstancePerSvc int) (ServiceInstanceMetrics, bool) {
	sim, ok := sm.ServiceInstanceGroups[siKey]
	if !ok {
		if len(sm.ServiceInstanceGroups) < maxSvcInstancePerSvc {
			return newServiceInstanceMetrics(), false
		}
		return ServiceInstanceMetrics{}, true
	}
	return sim, false
}

func newServiceMetrics() ServiceMetrics {
	return ServiceMetrics{
		ServiceInstanceGroups: make(map[ServiceInstanceAggregationKey]ServiceInstanceMetrics),
	}
}

func newServiceInstanceMetrics() ServiceInstanceMetrics {
	return ServiceInstanceMetrics{
		TransactionGroups:        make(map[TransactionAggregationKey]*aggregationpb.KeyedTransactionMetrics),
		ServiceTransactionGroups: make(map[ServiceTransactionAggregationKey]*aggregationpb.KeyedServiceTransactionMetrics),
		SpanGroups:               make(map[SpanAggregationKey]*aggregationpb.KeyedSpanMetrics),
	}
}

type constraint struct {
	counter int
	limit   int
}

func newConstraint(initialCount, limit int) *constraint {
	return &constraint{
		counter: initialCount,
		limit:   limit,
	}
}

func (c *constraint) maxed() bool {
	return c.counter >= c.limit
}

func (c *constraint) add(delta int) {
	c.counter += delta
}

func (c *constraint) value() int {
	return c.counter
}
