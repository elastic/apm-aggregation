// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"io"

	"github.com/axiomhq/hyperloglog"

	"github.com/elastic/apm-aggregation/aggregators/internal/hdrhistogram"
)

type combinedMetricsMerger struct {
	limits  Limits
	metrics CombinedMetrics
}

func (m *combinedMetricsMerger) MergeNewer(value []byte) error {
	var from CombinedMetrics
	if err := from.UnmarshalBinary(value); err != nil {
		return err
	}
	merge(&m.metrics, &from, m.limits)
	return nil
}

func (m *combinedMetricsMerger) MergeOlder(value []byte) error {
	var from CombinedMetrics
	if err := from.UnmarshalBinary(value); err != nil {
		return err
	}
	merge(&m.metrics, &from, m.limits)
	return nil
}

func (m *combinedMetricsMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	data, err := m.metrics.MarshalBinary()
	return data, nil, err
}

type Constraint struct {
	counter int
	limit   int
}

func newConstraint(initialCount, limit int) *Constraint {
	return &Constraint{
		counter: initialCount,
		limit:   limit,
	}
}

func (c *Constraint) maxed() bool {
	return c.counter >= c.limit
}

func (c *Constraint) add(delta int) {
	c.counter += delta
}

func (c *Constraint) value() int {
	return c.counter
}

// merge merges two combined metrics considering the configured limits.
func merge(to, from *CombinedMetrics, limits Limits) {
	// eventsTotal tracks the total number of events merged in a single combined metrics
	// irrespective of overflows. We merge the events total irrespective
	// of the services present because it is possible for services to be empty
	// if the event does not fit the criteria for aggregations.
	to.eventsTotal += from.eventsTotal
	if len(from.Services) == 0 {
		// Accounts for overflow too as overflow cannot happen with 0 entries.
		return
	}
	// If there is overflow due to max services in either of the buckets being
	// merged then we can merge the overflow buckets without considering any other scenarios.
	mergeOverflow(&to.OverflowServices, &from.OverflowServices)
	if from.OverflowServiceInstancesEstimator != nil {
		mergeEstimator(&to.OverflowServiceInstancesEstimator, from.OverflowServiceInstancesEstimator)
	}

	// Calculate the current capacity of the transaction, service transaction,
	// and span groups in the _to_ combined metrics.
	totalTransactionGroupsConstraint := newConstraint(0, limits.MaxTransactionGroups)
	totalServiceTransactionGroupsConstraint := newConstraint(0, limits.MaxServiceTransactionGroups)
	totalSpanGroupsConstraint := newConstraint(0, limits.MaxSpanGroups)
	for _, svc := range to.Services {
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
	for svcKey, fromSvc := range from.Services {
		hash := Hasher{}.Chain(svcKey)
		toSvc, svcOverflow := getServiceMetrics(to, svcKey, limits.MaxServices)
		if svcOverflow {
			mergeOverflow(&to.OverflowServices, &fromSvc.OverflowGroups)

			for sik, sim := range fromSvc.ServiceInstanceGroups {
				sikHash := hash.Chain(sik)
				mergeToOverflowFromSIM(&to.OverflowServices, &sim, sikHash)
				insertHash(&to.OverflowServiceInstancesEstimator, sikHash.Sum())
			}
			continue
		}
		mergeOverflow(&toSvc.OverflowGroups, &fromSvc.OverflowGroups)
		mergeServiceInstanceGroups(&toSvc, &fromSvc,
			totalTransactionGroupsConstraint, totalServiceTransactionGroupsConstraint, totalSpanGroupsConstraint,
			limits, hash, &to.OverflowServiceInstancesEstimator)
		to.Services[svcKey] = toSvc
	}
}

func mergeToOverflowFromSIM(to *Overflow, from *ServiceInstanceMetrics, hash Hasher) {
	for tk, tm := range from.TransactionGroups {
		to.OverflowTransaction.Merge(&tm, hash.Chain(tk).Sum())
	}
	for stk, stm := range from.ServiceTransactionGroups {
		to.OverflowServiceTransaction.Merge(&stm, hash.Chain(stk).Sum())
	}
	for sk, sm := range from.SpanGroups {
		to.OverflowSpan.Merge(&sm, hash.Chain(sk).Sum())
	}
}

func mergeServiceInstanceGroups(to, from *ServiceMetrics, totalTransactionGroupsConstraint, totalServiceTransactionGroupsConstraint, totalSpanGroupsConstraint *Constraint, limits Limits, hash Hasher, overflowServiceInstancesEstimator **hyperloglog.Sketch) {
	for siKey, fromSIM := range from.ServiceInstanceGroups {
		toSIM, overflowed := getServiceInstanceMetrics(to, siKey, limits.MaxServiceInstanceGroupsPerService)
		siKeyHash := hash.Chain(siKey)
		if overflowed {
			mergeToOverflowFromSIM(&to.OverflowGroups, &fromSIM, siKeyHash)
			insertHash(overflowServiceInstancesEstimator, siKeyHash.Sum())
			continue
		}

		mergeTransactionGroups(
			&toSIM,
			&fromSIM,
			newConstraint(len(toSIM.TransactionGroups), limits.MaxTransactionGroupsPerService),
			totalTransactionGroupsConstraint,
			hash,
			&to.OverflowGroups.OverflowTransaction,
		)
		mergeServiceTransactionGroups(
			&toSIM,
			&fromSIM,
			newConstraint(len(toSIM.ServiceTransactionGroups), limits.MaxServiceTransactionGroupsPerService),
			totalServiceTransactionGroupsConstraint,
			hash,
			&to.OverflowGroups.OverflowServiceTransaction,
		)
		mergeSpanGroups(
			&toSIM,
			&fromSIM,
			newConstraint(len(toSIM.SpanGroups), limits.MaxSpanGroupsPerService),
			totalSpanGroupsConstraint,
			hash,
			&to.OverflowGroups.OverflowSpan,
		)
		to.ServiceInstanceGroups[siKey] = toSIM
	}
}

// mergeTransactionGroups merges transaction aggregation groups for two combined metrics
// considering max transaction groups and max transaction groups per service limits.
func mergeTransactionGroups(to, from *ServiceInstanceMetrics, perSvcConstraint, globalConstraint *Constraint, hash Hasher, overflowTo *OverflowTransaction) {
	for txnKey, fromTxn := range from.TransactionGroups {
		toTxn, ok := to.TransactionGroups[txnKey]
		if !ok {
			overflowed := perSvcConstraint.maxed() || globalConstraint.maxed()
			if overflowed {
				overflowTo.Merge(&fromTxn, hash.Chain(txnKey).Sum())
				continue
			}
			toTxn = newTransactionMetrics()
			perSvcConstraint.add(1)
			globalConstraint.add(1)
		}
		mergeTransactionMetrics(&toTxn, &fromTxn)
		to.TransactionGroups[txnKey] = toTxn
	}
}

// mergeServiceTransactionGroups merges service transaction aggregation groups for two combined metrics
// considering max service transaction groups and max service transaction groups per service limits.
func mergeServiceTransactionGroups(to, from *ServiceInstanceMetrics, perSvcConstraint, globalConstraint *Constraint, hash Hasher, overflowTo *OverflowServiceTransaction) {
	for svcTxnKey, fromSvcTxn := range from.ServiceTransactionGroups {
		toSvcTxn, ok := to.ServiceTransactionGroups[svcTxnKey]
		if !ok {
			overflowed := perSvcConstraint.maxed() || globalConstraint.maxed()
			if overflowed {
				overflowTo.Merge(&fromSvcTxn, hash.Chain(svcTxnKey).Sum())
				continue
			}
			toSvcTxn = newServiceTransactionMetrics()
			perSvcConstraint.add(1)
			globalConstraint.add(1)
		}
		mergeServiceTransactionMetrics(&toSvcTxn, &fromSvcTxn)
		to.ServiceTransactionGroups[svcTxnKey] = toSvcTxn
	}
}

// mergeSpanGroups merges span aggregation groups for two combined metrics considering
// max span groups and max span groups per service limits.
func mergeSpanGroups(to, from *ServiceInstanceMetrics, perSvcConstraint, globalConstraint *Constraint, hash Hasher, overflowTo *OverflowSpan) {
	for spanKey, fromSpan := range from.SpanGroups {
		toSpan, ok := to.SpanGroups[spanKey]
		if !ok {
			// Protect against agents that send high cardinality span names by dropping
			// span.name if more than half of the per svc span group limit is reached.
			half := perSvcConstraint.limit / 2
			if perSvcConstraint.value() >= half {
				spanKey.SpanName = ""
				toSpan, ok = to.SpanGroups[spanKey]
			}
			if !ok {
				overflowed := perSvcConstraint.maxed() || globalConstraint.maxed()
				if overflowed {
					overflowTo.Merge(&fromSpan, hash.Chain(spanKey).Sum())
					continue
				}
				perSvcConstraint.add(1)
				globalConstraint.add(1)
			}
		}
		mergeSpanMetrics(&toSpan, &fromSpan)
		to.SpanGroups[spanKey] = toSpan
	}
}

// mergeOverflow merges overflowed aggregation groups for transaction,
// service transaction, and span groups.
func mergeOverflow(to, from *Overflow) {
	to.OverflowTransaction.MergeOverflow(&from.OverflowTransaction)
	to.OverflowServiceTransaction.MergeOverflow(&from.OverflowServiceTransaction)
	to.OverflowSpan.MergeOverflow(&from.OverflowSpan)
}

// mergeTransactionMetrics merges two transaction metrics.
func mergeTransactionMetrics(to, from *TransactionMetrics) {
	if to.Histogram == nil && from.Histogram != nil {
		to.Histogram = hdrhistogram.New()
	}
	to.Histogram.Merge(from.Histogram)
}

// mergeTransactionMetrics merges two transaction metrics.
func mergeServiceTransactionMetrics(to, from *ServiceTransactionMetrics) {
	if to.Histogram == nil && from.Histogram != nil {
		to.Histogram = hdrhistogram.New()
	}
	to.Histogram.Merge(from.Histogram)
	to.FailureCount += from.FailureCount
	to.SuccessCount += from.SuccessCount
}

// mergeSpanMetrics merges two span metrics.
func mergeSpanMetrics(to, from *SpanMetrics) {
	to.Count += from.Count
	to.Sum += from.Sum
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

func newTransactionMetrics() TransactionMetrics {
	return TransactionMetrics{
		Histogram: hdrhistogram.New(),
	}
}

func newServiceTransactionMetrics() ServiceTransactionMetrics {
	return ServiceTransactionMetrics{
		Histogram: hdrhistogram.New(),
	}
}

func newServiceInstanceMetrics() ServiceInstanceMetrics {
	return ServiceInstanceMetrics{
		TransactionGroups:        make(map[TransactionAggregationKey]TransactionMetrics),
		ServiceTransactionGroups: make(map[ServiceTransactionAggregationKey]ServiceTransactionMetrics),
		SpanGroups:               make(map[SpanAggregationKey]SpanMetrics),
	}
}

func newServiceMetrics() ServiceMetrics {
	return ServiceMetrics{
		ServiceInstanceGroups: make(map[ServiceInstanceAggregationKey]ServiceInstanceMetrics),
	}
}
