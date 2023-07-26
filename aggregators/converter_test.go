// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/elastic/apm-aggregation/aggregationpb"
	"github.com/elastic/apm-aggregation/aggregators/internal/hdrhistogram"
	"github.com/elastic/apm-data/model/modelpb"
)

func TestEventToCombinedMetrics(t *testing.T) {
	ts := time.Now().UTC()
	receivedTS := ts.Add(time.Second)
	baseEvent := &modelpb.APMEvent{
		Timestamp: timestamppb.New(ts),
		ParentId:  "nonroot",
		Service:   &modelpb.Service{Name: "test"},
		Event: &modelpb.Event{
			Duration: durationpb.New(time.Second),
			Outcome:  "success",
			Received: timestamppb.New(receivedTS),
		},
	}
	for _, tc := range []struct {
		name        string
		input       func() *modelpb.APMEvent
		partitioner Partitioner
		expected    func() []*aggregationpb.CombinedMetrics
	}{
		{
			name: "with-zero-rep-count-txn",
			input: func() *modelpb.APMEvent {
				event := baseEvent.CloneVT()
				event.Transaction = &modelpb.Transaction{
					Name:                "testtxn",
					Type:                "testtyp",
					RepresentativeCount: 0,
				}
				return event
			},
			partitioner: NewHashPartitioner(1),
			expected: func() []*aggregationpb.CombinedMetrics {
				return nil
			},
		},
		{
			name: "with-good-txn",
			input: func() *modelpb.APMEvent {
				event := baseEvent.CloneVT()
				event.Transaction = &modelpb.Transaction{
					Name:                "testtxn",
					Type:                "testtyp",
					RepresentativeCount: 1,
				}
				return event
			},
			partitioner: NewHashPartitioner(1),
			expected: func() []*aggregationpb.CombinedMetrics {
				return []*aggregationpb.CombinedMetrics{
					NewTestCombinedMetrics(
						WithEventsTotal(1),
						WithYoungestEventTimestamp(receivedTS)).
						AddServiceMetrics(ServiceAggregationKey{
							Timestamp:   ts.Truncate(time.Minute),
							ServiceName: "test"}).
						AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
						AddTransaction(TransactionAggregationKey{
							TransactionName: "testtxn",
							TransactionType: "testtyp",
							EventOutcome:    "success",
						}).
						AddServiceTransaction(ServiceTransactionAggregationKey{
							TransactionType: "testtyp",
						}).GetProto(),
				}
			},
		},
		{
			name: "with-zero-rep-count-span",
			input: func() *modelpb.APMEvent {
				event := baseEvent.CloneVT()
				event.Span = &modelpb.Span{
					Name:                "testspan",
					Type:                "testtyp",
					RepresentativeCount: 0,
				}
				return event
			},
			partitioner: NewHashPartitioner(1),
			expected: func() []*aggregationpb.CombinedMetrics {
				return nil
			},
		},
		{
			name: "with-no-exit-span",
			input: func() *modelpb.APMEvent {
				event := baseEvent.CloneVT()
				event.Span = &modelpb.Span{
					Name:                "testspan",
					Type:                "testtyp",
					RepresentativeCount: 1,
				}
				return event
			},
			partitioner: NewHashPartitioner(1),
			expected: func() []*aggregationpb.CombinedMetrics {
				return nil
			},
		},
		{
			name: "with-good-span-svc-target",
			input: func() *modelpb.APMEvent {
				event := baseEvent.CloneVT()
				event.Span = &modelpb.Span{
					Name:                "testspan",
					Type:                "testtyp",
					RepresentativeCount: 1,
				}
				event.Service.Target = &modelpb.ServiceTarget{
					Name: "psql",
					Type: "db",
				}
				// Current test structs are hardcoded to use 1ns for spans
				event.Event.Duration = durationpb.New(time.Nanosecond)
				return event
			},
			partitioner: NewHashPartitioner(1),
			expected: func() []*aggregationpb.CombinedMetrics {
				return []*aggregationpb.CombinedMetrics{
					NewTestCombinedMetrics(
						WithEventsTotal(1),
						WithYoungestEventTimestamp(receivedTS)).
						AddServiceMetrics(ServiceAggregationKey{
							Timestamp:   ts.Truncate(time.Minute),
							ServiceName: "test"}).
						AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
						AddSpan(SpanAggregationKey{
							SpanName:   "testspan",
							TargetName: "psql",
							TargetType: "db",
							Outcome:    "success",
						}).GetProto(),
				}
			},
		},
		{
			name: "with-good-span-dest-svc",
			input: func() *modelpb.APMEvent {
				event := baseEvent.CloneVT()
				event.Span = &modelpb.Span{
					Name:                "testspan",
					Type:                "testtyp",
					RepresentativeCount: 1,
					DestinationService: &modelpb.DestinationService{
						Resource: "db",
					},
				}
				// Current test structs are hardcoded to use 1ns for spans
				event.Event.Duration = durationpb.New(time.Nanosecond)
				return event
			},
			partitioner: NewHashPartitioner(1),
			expected: func() []*aggregationpb.CombinedMetrics {
				return []*aggregationpb.CombinedMetrics{
					NewTestCombinedMetrics(
						WithEventsTotal(1),
						WithYoungestEventTimestamp(receivedTS)).
						AddServiceMetrics(ServiceAggregationKey{
							Timestamp:   ts.Truncate(time.Minute),
							ServiceName: "test"}).
						AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
						AddSpan(SpanAggregationKey{
							SpanName: "testspan",
							Resource: "db",
							Outcome:  "success",
						}).GetProto(),
				}
			},
		},
		{
			name: "with-metricset",
			input: func() *modelpb.APMEvent {
				event := baseEvent.CloneVT()
				event.Metricset = &modelpb.Metricset{
					Name:     "testmetricset",
					Interval: "1m",
				}
				return event
			},
			partitioner: NewHashPartitioner(1),
			expected: func() []*aggregationpb.CombinedMetrics {
				return []*aggregationpb.CombinedMetrics{
					NewTestCombinedMetrics(
						WithEventsTotal(1),
						WithYoungestEventTimestamp(receivedTS)).
						AddServiceMetrics(ServiceAggregationKey{
							Timestamp:   ts.Truncate(time.Minute),
							ServiceName: "test"}).
						AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
						GetProto(),
				}
			},
		},
		{
			name: "with-log",
			input: func() *modelpb.APMEvent {
				event := baseEvent.CloneVT()
				event.Log = &modelpb.Log{}
				return event
			},
			partitioner: NewHashPartitioner(1),
			expected: func() []*aggregationpb.CombinedMetrics {
				return []*aggregationpb.CombinedMetrics{
					NewTestCombinedMetrics(
						WithEventsTotal(1),
						WithYoungestEventTimestamp(receivedTS)).
						AddServiceMetrics(ServiceAggregationKey{
							Timestamp:   ts.Truncate(time.Minute),
							ServiceName: "test"}).
						AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
						GetProto(),
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmk := CombinedMetricsKey{
				Interval:       time.Minute,
				ProcessingTime: time.Now().Truncate(time.Minute),
				ID:             EncodeToCombinedMetricsKeyID(t, "ab01"),
			}
			var actual []*aggregationpb.CombinedMetrics
			collector := func(
				_ CombinedMetricsKey,
				m *aggregationpb.CombinedMetrics,
			) error {
				actual = append(actual, m.CloneVT())
				return nil
			}
			err := EventToCombinedMetrics(tc.input(), cmk, tc.partitioner, collector)
			require.NoError(t, err)
			assert.Empty(t, cmp.Diff(
				tc.expected(), actual,
				cmp.Comparer(func(a, b hdrhistogram.HybridCountsRep) bool {
					return a.Equal(&b)
				}),
				protocmp.Transform(),
				protocmp.IgnoreEmptyMessages(),
			))
		})
	}
}

// func TestCombinedMetricsToBatch(t *testing.T) {
// 	ts := time.Now()
// 	aggIvl := time.Minute
// 	processingTime := ts.Truncate(aggIvl)
// 	svcName := "test"
// 	coldstart := true
// 	var (
// 		faas           = &modelpb.Faas{Id: "f1", ColdStart: &coldstart, Version: "v2", TriggerType: "http"}
// 		txn            = testTransaction{txnName: "txn", txnType: "typ", count: 100}
// 		txnFaas        = testTransaction{txnName: "txn", txnType: "typ", count: 100, faas: faas}
// 		svcTxn         = testServiceTransaction{txnType: "typ", count: 100}
// 		span           = testSpan{spanName: "spn", destinationResource: "postgresql", count: 1}
// 		overflowTxn    = testTransaction{txnName: "_other", count: 100}
// 		overflowSvcTxn = testServiceTransaction{txnType: "_other", count: 100}
// 		overflowSpan   = testSpan{targetName: "_other", count: 1}
// 	)
// 	for _, tc := range []struct {
// 		name                string
// 		aggregationInterval time.Duration
// 		combinedMetrics     CombinedMetrics
// 		expectedEvents      modelpb.Batch
// 	}{
// 		{
// 			name:                "no_overflow_without_faas",
// 			aggregationInterval: aggIvl,
// 			combinedMetrics: CombinedMetrics(
// 				*createTestCombinedMetrics().
// 					addTransaction(ts, svcName, "", txn).
// 					addServiceTransaction(ts, svcName, "", svcTxn).
// 					addSpan(ts, svcName, "", span),
// 			),
// 			expectedEvents: []*modelpb.APMEvent{
// 				createTestTransactionMetric(ts, aggIvl, svcName, txn, 0),
// 				createTestServiceTransactionMetric(ts, aggIvl, svcName, svcTxn, 0),
// 				createTestSpanMetric(ts, aggIvl, svcName, span, 0),
// 				createTestServiceSummaryMetric(ts, aggIvl, svcName, 0),
// 			},
// 		},
// 		{
// 			name:                "no_overflow",
// 			aggregationInterval: aggIvl,
// 			combinedMetrics: CombinedMetrics(
// 				*createTestCombinedMetrics().
// 					addTransaction(ts, svcName, "", txnFaas).
// 					addServiceTransaction(ts, svcName, "", svcTxn).
// 					addSpan(ts, svcName, "", span),
// 			),
// 			expectedEvents: []*modelpb.APMEvent{
// 				createTestTransactionMetric(ts, aggIvl, svcName, txnFaas, 0),
// 				createTestServiceTransactionMetric(ts, aggIvl, svcName, svcTxn, 0),
// 				createTestSpanMetric(ts, aggIvl, svcName, span, 0),
// 				createTestServiceSummaryMetric(ts, aggIvl, svcName, 0),
// 			},
// 		},
// 		{
// 			name:                "overflow",
// 			aggregationInterval: aggIvl,
// 			combinedMetrics: CombinedMetrics(
// 				*createTestCombinedMetrics().
// 					addTransaction(ts, svcName, "", txnFaas).
// 					addServiceTransaction(ts, svcName, "", svcTxn).
// 					addSpan(ts, svcName, "", span).
// 					addPerServiceOverflowTransaction(ts, svcName, "", txn).
// 					addPerServiceOverflowServiceTransaction(ts, svcName, "", svcTxn).
// 					addPerServiceOverflowSpan(ts, svcName, "", span).
// 					addGlobalServiceOverflowServiceInstance(ts, "overflow", ""),
// 			),
// 			expectedEvents: []*modelpb.APMEvent{
// 				createTestTransactionMetric(ts, aggIvl, svcName, txnFaas, 0),
// 				createTestServiceTransactionMetric(ts, aggIvl, svcName, svcTxn, 0),
// 				createTestSpanMetric(ts, aggIvl, svcName, span, 0),
// 				createTestServiceSummaryMetric(ts, aggIvl, svcName, 0),
// 				// Events due to overflow
// 				createTestTransactionMetric(processingTime, aggIvl, svcName, overflowTxn, 1),
// 				createTestServiceTransactionMetric(processingTime, aggIvl, svcName, overflowSvcTxn, 1),
// 				createTestSpanMetric(processingTime, aggIvl, svcName, overflowSpan, 1),
// 				createTestServiceSummaryMetric(processingTime, aggIvl, "_other", 1),
// 			},
// 		},
// 		{
// 			name:                "service_instance_overflow_in_global_and_per_svc",
// 			aggregationInterval: aggIvl,
// 			combinedMetrics: CombinedMetrics(
// 				*createTestCombinedMetrics().
// 					addServiceInstance(ts, "svc1", "").
// 					addGlobalServiceOverflowServiceInstance(ts, "svc1", "1").
// 					addGlobalServiceOverflowServiceInstance(ts, "svc2", "1"),
// 			),
// 			expectedEvents: []*modelpb.APMEvent{
// 				createTestServiceSummaryMetric(ts, aggIvl, "svc1", 0),
// 				createTestServiceSummaryMetric(processingTime, aggIvl, "_other", 2),
// 			},
// 		},
// 	} {
// 		t.Run(tc.name, func(t *testing.T) {
// 			b, err := CombinedMetricsToBatch(
// 				tc.combinedMetrics,
// 				processingTime,
// 				tc.aggregationInterval,
// 			)
// 			assert.NoError(t, err)
// 			assert.Empty(t, cmp.Diff(
// 				tc.expectedEvents, *b,
// 				cmpopts.IgnoreTypes(netip.Addr{}),
// 				cmpopts.SortSlices(func(e1, e2 *modelpb.APMEvent) bool {
// 					m1Name := e1.GetMetricset().GetName()
// 					m2Name := e2.GetMetricset().GetName()
// 					if m1Name != m2Name {
// 						return m1Name < m2Name
// 					}
//
// 					a1Name := e1.GetAgent().GetName()
// 					a2Name := e2.GetAgent().GetName()
// 					if a1Name != a2Name {
// 						return a1Name < a2Name
// 					}
//
// 					return e1.GetService().GetEnvironment() < e2.GetService().GetEnvironment()
// 				}),
// 				protocmp.Transform(),
// 			))
// 		})
// 	}
// }

func BenchmarkCombinedMetricsToBatch(b *testing.B) {
	ai := time.Hour
	ts := time.Now()
	pt := ts.Truncate(ai)
	cardinality := 10
	tcm := NewTestCombinedMetrics().
		AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "bench"}).
		AddServiceInstanceMetrics(ServiceInstanceAggregationKey{})
	for i := 0; i < cardinality; i++ {
		txnName := fmt.Sprintf("txn%d", i)
		txnType := fmt.Sprintf("typ%d", i)
		spanName := fmt.Sprintf("spn%d", i)
		tcm.
			AddTransaction(TransactionAggregationKey{
				TransactionName: txnName,
				TransactionType: txnType,
			}, WithTransactionCount(200)).
			AddServiceTransaction(ServiceTransactionAggregationKey{
				TransactionType: txnType,
			}, WithTransactionCount(200)).
			AddSpan(SpanAggregationKey{
				SpanName: spanName,
			})
	}
	cm := tcm.Get()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := CombinedMetricsToBatch(cm, pt, ai)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEventToCombinedMetrics(b *testing.B) {
	event := &modelpb.APMEvent{
		Timestamp: timestamppb.Now(),
		ParentId:  "nonroot",
		Service: &modelpb.Service{
			Name: "test",
		},
		Event: &modelpb.Event{
			Duration: durationpb.New(time.Second),
			Outcome:  "success",
		},
		Transaction: &modelpb.Transaction{
			RepresentativeCount: 1,
			Name:                "testtxn",
			Type:                "testtyp",
		},
	}
	cmk := CombinedMetricsKey{
		Interval:       time.Minute,
		ProcessingTime: time.Now().Truncate(time.Minute),
		ID:             EncodeToCombinedMetricsKeyID(b, "ab01"),
	}
	partitioner := NewHashPartitioner(1)
	noop := func(_ CombinedMetricsKey, _ *aggregationpb.CombinedMetrics) error {
		return nil
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := EventToCombinedMetrics(event, cmk, partitioner, noop)
		if err != nil {
			b.Fatal(err)
		}
	}
}

//
// func createTestServiceSummaryMetric(
// 	ts time.Time,
// 	ivl time.Duration,
// 	svcName string,
// 	overflowCount int,
// ) *modelpb.APMEvent {
// 	var metricsetSamples []*modelpb.MetricsetSample
// 	if overflowCount > 0 {
// 		metricsetSamples = []*modelpb.MetricsetSample{
// 			{
// 				Name:  "service_summary.aggregation.overflow_count",
// 				Value: float64(overflowCount),
// 			},
// 		}
// 	}
// 	return &modelpb.APMEvent{
// 		Timestamp: timestamppb.New(ts),
// 		Metricset: &modelpb.Metricset{
// 			Name:     "service_summary",
// 			Samples:  metricsetSamples,
// 			Interval: formatDuration(ivl),
// 		},
// 		Service: &modelpb.Service{Name: svcName},
// 	}
// }
//
// func createTestTransactionMetric(
// 	ts time.Time,
// 	ivl time.Duration,
// 	svcName string,
// 	txn testTransaction,
// 	overflowCount int,
// ) *modelpb.APMEvent {
// 	histRep := hdrhistogram.New()
// 	for i := 0; i < txn.count; i++ {
// 		histRep.RecordDuration(time.Second, 1)
// 	}
//
// 	total, counts, values := histRep.Buckets()
// 	var eventSuccessSummary modelpb.SummaryMetric
// 	switch txn.eventOutcome {
// 	case "success":
// 		eventSuccessSummary.Count = total
// 		eventSuccessSummary.Sum = float64(total)
// 	case "failure":
// 		eventSuccessSummary.Count = total
// 	case "unknown":
// 		// Keep both Count and Sum as 0.
// 	}
// 	transactionDurationSummary := &modelpb.SummaryMetric{
// 		Count: total,
// 		// only 1 expected element
// 		Sum: values[0] * float64(counts[0]),
// 	}
// 	var metricsetSamples []*modelpb.MetricsetSample
// 	if overflowCount > 0 {
// 		metricsetSamples = []*modelpb.MetricsetSample{
// 			{
// 				Name:  "transaction.aggregation.overflow_count",
// 				Value: float64(overflowCount),
// 			},
// 		}
// 	}
// 	return &modelpb.APMEvent{
// 		Timestamp: timestamppb.New(ts),
// 		Metricset: &modelpb.Metricset{
// 			Name:     "transaction",
// 			Interval: formatDuration(ivl),
// 			Samples:  metricsetSamples,
// 			DocCount: total,
// 		},
// 		Service: &modelpb.Service{Name: svcName},
// 		Transaction: &modelpb.Transaction{
// 			Name: txn.txnName,
// 			Type: txn.txnType,
// 			DurationHistogram: &modelpb.Histogram{
// 				Counts: counts,
// 				Values: values,
// 			},
// 			DurationSummary: transactionDurationSummary,
// 		},
// 		Faas: txn.faas,
// 		Event: &modelpb.Event{
// 			SuccessCount: &eventSuccessSummary,
// 		},
// 	}
// }
//
// func createTestServiceTransactionMetric(
// 	ts time.Time,
// 	ivl time.Duration,
// 	svcName string,
// 	svcTxn testServiceTransaction,
// 	overflowCount int,
// ) *modelpb.APMEvent {
// 	histRep := hdrhistogram.New()
// 	for i := 0; i < svcTxn.count; i++ {
// 		histRep.RecordDuration(time.Second, 1)
// 	}
// 	total, counts, values := histRep.Buckets()
// 	transactionDurationSummary := &modelpb.SummaryMetric{
// 		Count: total,
// 		// only 1 expected element
// 		Sum: values[0] * float64(counts[0]),
// 	}
// 	var metricsetSamples []*modelpb.MetricsetSample
// 	if overflowCount > 0 {
// 		metricsetSamples = []*modelpb.MetricsetSample{
// 			{
// 				Name:  "service_transaction.aggregation.overflow_count",
// 				Value: float64(overflowCount),
// 			},
// 		}
// 	}
// 	return &modelpb.APMEvent{
// 		Timestamp: timestamppb.New(ts),
// 		Metricset: &modelpb.Metricset{
// 			Name:     "service_transaction",
// 			Interval: formatDuration(ivl),
// 			Samples:  metricsetSamples,
// 			DocCount: total,
// 		},
// 		Service: &modelpb.Service{Name: svcName},
// 		Transaction: &modelpb.Transaction{
// 			Type: svcTxn.txnType,
// 			DurationHistogram: &modelpb.Histogram{
// 				Counts: counts,
// 				Values: values,
// 			},
// 			DurationSummary: transactionDurationSummary,
// 		},
// 		Event: &modelpb.Event{
// 			SuccessCount: &modelpb.SummaryMetric{
// 				// test code generates all success events
// 				Count: int64(svcTxn.count),
// 				Sum:   float64(svcTxn.count),
// 			},
// 		},
// 	}
// }
//
// func createTestSpanMetric(
// 	ts time.Time,
// 	ivl time.Duration,
// 	svcName string,
// 	span testSpan,
// 	overflowCount int,
// ) *modelpb.APMEvent {
// 	var metricsetSamples []*modelpb.MetricsetSample
// 	if overflowCount > 0 {
// 		metricsetSamples = []*modelpb.MetricsetSample{
// 			{
// 				Name:  "service_destination.aggregation.overflow_count",
// 				Value: float64(overflowCount),
// 			},
// 		}
// 	}
// 	var target *modelpb.ServiceTarget
// 	if span.targetName != "" {
// 		target = &modelpb.ServiceTarget{
// 			Name: span.targetName,
// 		}
// 	}
// 	return &modelpb.APMEvent{
// 		Timestamp: timestamppb.New(ts),
// 		Metricset: &modelpb.Metricset{
// 			Name:     "service_destination",
// 			Interval: formatDuration(ivl),
// 			Samples:  metricsetSamples,
// 			DocCount: int64(span.count),
// 		},
// 		Service: &modelpb.Service{
// 			Name:   svcName,
// 			Target: target,
// 		},
// 		Span: &modelpb.Span{
// 			Name: span.spanName,
// 			DestinationService: &modelpb.DestinationService{
// 				Resource: span.destinationResource,
// 				ResponseTime: &modelpb.AggregatedDuration{
// 					// test code generates 1 count for 1 ns
// 					Count: int64(span.count),
// 					Sum:   durationpb.New(time.Duration(span.count)),
// 				},
// 			},
// 		},
// 	}
// }
