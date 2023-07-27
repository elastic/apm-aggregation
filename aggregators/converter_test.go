// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/elastic/apm-aggregation/aggregationpb"
	"github.com/elastic/apm-aggregation/aggregators/internal/hdrhistogram"
	"github.com/elastic/apm-aggregation/aggregators/nullable"
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

func TestCombinedMetricsToBatch(t *testing.T) {
	ts := time.Now()
	aggIvl := time.Minute
	processingTime := ts.Truncate(aggIvl)
	svcName := "test"
	coldstart := nullable.True
	var (
		svc            = ServiceAggregationKey{Timestamp: ts, ServiceName: svcName}
		svcIns         = ServiceInstanceAggregationKey{}
		faas           = &modelpb.Faas{Id: "f1", ColdStart: coldstart.ToBoolPtr(), Version: "v2", TriggerType: "http"}
		span           = SpanAggregationKey{SpanName: "spn", Resource: "postgresql"}
		overflowSpan   = SpanAggregationKey{TargetName: "_other"}
		spanCount      = 1
		svcTxn         = ServiceTransactionAggregationKey{TransactionType: "typ"}
		overflowSvcTxn = ServiceTransactionAggregationKey{TransactionType: "_other"}
		txn            = TransactionAggregationKey{TransactionName: "txn", TransactionType: "typ"}
		txnFaas        = TransactionAggregationKey{TransactionName: "txn", TransactionType: "typ",
			FAASID: faas.Id, FAASColdstart: coldstart, FAASVersion: faas.Version, FAASTriggerType: faas.TriggerType}
		overflowTxn = TransactionAggregationKey{TransactionName: "_other"}
		txnCount    = 100
	)
	for _, tc := range []struct {
		name                string
		aggregationInterval time.Duration
		combinedMetrics     func() CombinedMetrics
		expectedEvents      modelpb.Batch
	}{
		{
			name:                "no_overflow_without_faas",
			aggregationInterval: aggIvl,
			combinedMetrics: func() CombinedMetrics {
				return NewTestCombinedMetrics().
					AddServiceMetrics(svc).
					AddServiceInstanceMetrics(svcIns).
					AddSpan(span, WithSpanCount(spanCount)).
					AddTransaction(txn, WithTransactionCount(txnCount)).
					AddServiceTransaction(svcTxn, WithTransactionCount(txnCount)).
					Get()
			},
			expectedEvents: []*modelpb.APMEvent{
				createTestTransactionMetric(ts, aggIvl, svcName, txn, nil, txnCount, 0),
				createTestServiceTransactionMetric(ts, aggIvl, svcName, svcTxn, txnCount, 0),
				createTestSpanMetric(ts, aggIvl, svcName, span, spanCount, 0),
				createTestServiceSummaryMetric(ts, aggIvl, svcName, 0),
			},
		},
		{
			name:                "no_overflow",
			aggregationInterval: aggIvl,
			combinedMetrics: func() CombinedMetrics {
				return NewTestCombinedMetrics().
					AddServiceMetrics(svc).
					AddServiceInstanceMetrics(svcIns).
					AddSpan(span, WithSpanCount(spanCount)).
					AddTransaction(txnFaas, WithTransactionCount(txnCount)).
					AddServiceTransaction(svcTxn, WithTransactionCount(txnCount)).
					Get()
			},
			expectedEvents: []*modelpb.APMEvent{
				createTestTransactionMetric(ts, aggIvl, svcName, txn, faas, txnCount, 0),
				createTestServiceTransactionMetric(ts, aggIvl, svcName, svcTxn, txnCount, 0),
				createTestSpanMetric(ts, aggIvl, svcName, span, spanCount, 0),
				createTestServiceSummaryMetric(ts, aggIvl, svcName, 0),
			},
		},
		{
			name:                "overflow",
			aggregationInterval: aggIvl,
			combinedMetrics: func() CombinedMetrics {
				tcm := NewTestCombinedMetrics()
				tcm.
					AddServiceMetrics(svc).
					AddServiceInstanceMetrics(svcIns).
					AddSpan(span, WithSpanCount(spanCount)).
					AddTransaction(txnFaas, WithTransactionCount(txnCount)).
					AddServiceTransaction(svcTxn, WithTransactionCount(txnCount)).
					AddTransactionOverflow(txn, WithTransactionCount(txnCount)).
					AddServiceTransactionOverflow(svcTxn, WithTransactionCount(txnCount)).
					AddSpanOverflow(span, WithSpanCount(spanCount))
				// Add global service overflow
				tcm.
					AddServiceMetricsOverflow(
						ServiceAggregationKey{Timestamp: ts, ServiceName: "svc_overflow"}).
					AddServiceInstanceMetricsOverflow(ServiceInstanceAggregationKey{})
				return tcm.Get()
			},
			expectedEvents: []*modelpb.APMEvent{
				createTestTransactionMetric(ts, aggIvl, svcName, txnFaas, faas, txnCount, 0),
				createTestServiceTransactionMetric(ts, aggIvl, svcName, svcTxn, txnCount, 0),
				createTestSpanMetric(ts, aggIvl, svcName, span, spanCount, 0),
				createTestServiceSummaryMetric(ts, aggIvl, svcName, 0),
				// Events due to overflow
				createTestTransactionMetric(processingTime, aggIvl, svcName, overflowTxn, nil, txnCount, 1),
				createTestServiceTransactionMetric(processingTime, aggIvl, svcName, overflowSvcTxn, txnCount, 1),
				createTestSpanMetric(processingTime, aggIvl, svcName, overflowSpan, spanCount, 1),
				createTestServiceSummaryMetric(processingTime, aggIvl, "_other", 1),
			},
		},
		{
			name:                "service_instance_overflow_in_global_and_per_svc",
			aggregationInterval: aggIvl,
			combinedMetrics: func() CombinedMetrics {
				tcm := NewTestCombinedMetrics()
				tcm.
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{})
				tcm.
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetricsOverflow(ServiceInstanceAggregationKey{GlobalLabelsStr: getTestGlobalLabelsStr(t, "1")})
				tcm.
					AddServiceMetricsOverflow(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc2"}).
					AddServiceInstanceMetricsOverflow(ServiceInstanceAggregationKey{GlobalLabelsStr: getTestGlobalLabelsStr(t, "2")})
				return tcm.Get()
			},
			expectedEvents: []*modelpb.APMEvent{
				createTestServiceSummaryMetric(ts, aggIvl, "svc1", 0),
				createTestServiceSummaryMetric(processingTime, aggIvl, "_other", 2),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := CombinedMetricsToBatch(
				tc.combinedMetrics(),
				processingTime,
				tc.aggregationInterval,
			)
			assert.NoError(t, err)
			assert.Empty(t, cmp.Diff(
				tc.expectedEvents, *b,
				cmpopts.IgnoreTypes(netip.Addr{}),
				cmpopts.SortSlices(func(e1, e2 *modelpb.APMEvent) bool {
					m1Name := e1.GetMetricset().GetName()
					m2Name := e2.GetMetricset().GetName()
					if m1Name != m2Name {
						return m1Name < m2Name
					}

					a1Name := e1.GetAgent().GetName()
					a2Name := e2.GetAgent().GetName()
					if a1Name != a2Name {
						return a1Name < a2Name
					}

					return e1.GetService().GetEnvironment() < e2.GetService().GetEnvironment()
				}),
				protocmp.Transform(),
			))
		})
	}
}

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

func createTestServiceSummaryMetric(
	ts time.Time,
	ivl time.Duration,
	svcName string,
	overflowCount int,
) *modelpb.APMEvent {
	var metricsetSamples []*modelpb.MetricsetSample
	if overflowCount > 0 {
		metricsetSamples = []*modelpb.MetricsetSample{
			{
				Name:  "service_summary.aggregation.overflow_count",
				Value: float64(overflowCount),
			},
		}
	}
	return &modelpb.APMEvent{
		Timestamp: timestamppb.New(ts),
		Metricset: &modelpb.Metricset{
			Name:     "service_summary",
			Samples:  metricsetSamples,
			Interval: formatDuration(ivl),
		},
		Service: &modelpb.Service{Name: svcName},
	}
}

func createTestTransactionMetric(
	ts time.Time,
	ivl time.Duration,
	svcName string,
	txn TransactionAggregationKey,
	faas *modelpb.Faas,
	count, overflowCount int,
) *modelpb.APMEvent {
	histRep := hdrhistogram.New()
	histRep.RecordDuration(time.Second, float64(count))
	total, counts, values := histRep.Buckets()
	var eventSuccessSummary modelpb.SummaryMetric
	switch txn.EventOutcome {
	case "success":
		eventSuccessSummary.Count = total
		eventSuccessSummary.Sum = float64(total)
	case "failure":
		eventSuccessSummary.Count = total
	case "unknown":
		// Keep both Count and Sum as 0.
	}
	transactionDurationSummary := &modelpb.SummaryMetric{
		Count: total,
		// only 1 expected element
		Sum: values[0] * float64(counts[0]),
	}
	var metricsetSamples []*modelpb.MetricsetSample
	if overflowCount > 0 {
		metricsetSamples = []*modelpb.MetricsetSample{
			{
				Name:  "transaction.aggregation.overflow_count",
				Value: float64(overflowCount),
			},
		}
	}
	return &modelpb.APMEvent{
		Timestamp: timestamppb.New(ts),
		Metricset: &modelpb.Metricset{
			Name:     "transaction",
			Interval: formatDuration(ivl),
			Samples:  metricsetSamples,
			DocCount: total,
		},
		Service: &modelpb.Service{Name: svcName},
		Transaction: &modelpb.Transaction{
			Name: txn.TransactionName,
			Type: txn.TransactionType,
			DurationHistogram: &modelpb.Histogram{
				Counts: counts,
				Values: values,
			},
			DurationSummary: transactionDurationSummary,
		},
		Faas: faas,
		Event: &modelpb.Event{
			SuccessCount: &eventSuccessSummary,
		},
	}
}

func createTestServiceTransactionMetric(
	ts time.Time,
	ivl time.Duration,
	svcName string,
	svcTxn ServiceTransactionAggregationKey,
	count, overflowCount int,
) *modelpb.APMEvent {
	histRep := hdrhistogram.New()
	histRep.RecordDuration(time.Second, float64(count))
	total, counts, values := histRep.Buckets()
	transactionDurationSummary := &modelpb.SummaryMetric{
		Count: total,
		// only 1 expected element
		Sum: values[0] * float64(counts[0]),
	}
	var metricsetSamples []*modelpb.MetricsetSample
	if overflowCount > 0 {
		metricsetSamples = []*modelpb.MetricsetSample{
			{
				Name:  "service_transaction.aggregation.overflow_count",
				Value: float64(overflowCount),
			},
		}
	}
	return &modelpb.APMEvent{
		Timestamp: timestamppb.New(ts),
		Metricset: &modelpb.Metricset{
			Name:     "service_transaction",
			Interval: formatDuration(ivl),
			Samples:  metricsetSamples,
			DocCount: total,
		},
		Service: &modelpb.Service{Name: svcName},
		Transaction: &modelpb.Transaction{
			Type: svcTxn.TransactionType,
			DurationHistogram: &modelpb.Histogram{
				Counts: counts,
				Values: values,
			},
			DurationSummary: transactionDurationSummary,
		},
		Event: &modelpb.Event{
			SuccessCount: &modelpb.SummaryMetric{
				// test code generates all success events
				Count: int64(count),
				Sum:   float64(count),
			},
		},
	}
}

func createTestSpanMetric(
	ts time.Time,
	ivl time.Duration,
	svcName string,
	span SpanAggregationKey,
	count, overflowCount int,
) *modelpb.APMEvent {
	var metricsetSamples []*modelpb.MetricsetSample
	if overflowCount > 0 {
		metricsetSamples = []*modelpb.MetricsetSample{
			{
				Name:  "service_destination.aggregation.overflow_count",
				Value: float64(overflowCount),
			},
		}
	}
	var target *modelpb.ServiceTarget
	if span.TargetName != "" {
		target = &modelpb.ServiceTarget{
			Name: span.TargetName,
		}
	}
	return &modelpb.APMEvent{
		Timestamp: timestamppb.New(ts),
		Metricset: &modelpb.Metricset{
			Name:     "service_destination",
			Interval: formatDuration(ivl),
			Samples:  metricsetSamples,
			DocCount: int64(count),
		},
		Service: &modelpb.Service{
			Name:   svcName,
			Target: target,
		},
		Span: &modelpb.Span{
			Name: span.SpanName,
			DestinationService: &modelpb.DestinationService{
				Resource: span.Resource,
				ResponseTime: &modelpb.AggregatedDuration{
					// test code generates 1 count for 1 ns
					Count: int64(count),
					Sum:   durationpb.New(time.Duration(count)),
				},
			},
		},
	}
}

func getTestGlobalLabelsStr(t *testing.T, s string) string {
	t.Helper()
	var gl GlobalLabels
	gl.Labels = make(modelpb.Labels)
	gl.Labels["test"] = &modelpb.LabelValue{Value: s}
	gls, err := gl.MarshalString()
	if err != nil {
		t.Fatal(err)
	}
	return gls
}
