// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"

	"github.com/elastic/apm-aggregation/aggregators/internal/hdrhistogram"
	"github.com/elastic/apm-data/model/modelpb"
)

func TestCombinedMetricsKey(t *testing.T) {
	expected := CombinedMetricsKey{
		Interval:       time.Minute,
		ProcessingTime: time.Now().Truncate(time.Minute),
		ID:             EncodeToCombinedMetricsKeyID(t, "ab01"),
	}
	data := make([]byte, expected.SizeBinary())
	assert.NoError(t, expected.MarshalBinaryToSizedBuffer(data))
	var actual CombinedMetricsKey
	assert.NoError(t, (&actual).UnmarshalBinary(data))
	assert.Empty(t, cmp.Diff(expected, actual))
}

func TestGlobalLabels(t *testing.T) {
	expected := GlobalLabels{
		Labels: map[string]*modelpb.LabelValue{
			"lb01": {
				Values: []string{"test01", "test02"},
				Global: true,
			},
		},
		NumericLabels: map[string]*modelpb.NumericLabelValue{
			"nlb01": {
				Values: []float64{0.1, 0.2},
				Global: true,
			},
		},
	}
	str, err := expected.MarshalString()
	assert.NoError(t, err)
	var actual GlobalLabels
	assert.NoError(t, actual.UnmarshalString(str))
	assert.Empty(t, cmp.Diff(
		expected, actual,
		cmpopts.IgnoreUnexported(
			modelpb.LabelValue{},
			modelpb.NumericLabelValue{},
		),
	))
}

func TestHistogramRepresentation(t *testing.T) {
	expected := hdrhistogram.New()
	expected.RecordDuration(time.Minute, 2)

	actual := hdrhistogram.New()
	HistogramFromProto(actual, HistogramToProto(expected))
	assert.Empty(t, cmp.Diff(
		expected, actual,
		cmp.Comparer(func(a, b hdrhistogram.HybridCountsRep) bool {
			return a.Equal(&b)
		}),
	))
}

func BenchmarkCombinedMetricsEncoding(b *testing.B) {
	b.ReportAllocs()
	ts := time.Now()
	cardinality := 10
	tcm := NewTestCombinedMetrics()
	sim := tcm.AddServiceMetrics(ServiceAggregationKey{
		Timestamp:   ts,
		ServiceName: "bench",
	}).AddServiceInstanceMetrics(ServiceInstanceAggregationKey{})
	for i := 0; i < cardinality; i++ {
		txnName := fmt.Sprintf("txn%d", i)
		txnType := fmt.Sprintf("typ%d", i)
		spanName := fmt.Sprintf("spn%d", i)

		sim.AddTransaction(TransactionAggregationKey{
			TransactionName: txnName,
			TransactionType: txnType,
		}, WithTransactionCount(200))
		sim.AddServiceTransaction(ServiceTransactionAggregationKey{
			TransactionType: txnType,
		}, WithTransactionCount(200))
		sim.AddSpan(SpanAggregationKey{
			SpanName: spanName,
		})
	}
	cm := tcm.Get()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmproto := cm.ToProto()
		cmproto.ReturnToVTPool()
	}
}

func BenchmarkCombinedMetricsDecoding(b *testing.B) {
	b.ReportAllocs()
	ts := time.Now()
	cardinality := 10
	tcm := NewTestCombinedMetrics()
	sim := tcm.AddServiceMetrics(ServiceAggregationKey{
		Timestamp:   ts,
		ServiceName: "bench",
	}).AddServiceInstanceMetrics(ServiceInstanceAggregationKey{})
	for i := 0; i < cardinality; i++ {
		txnName := fmt.Sprintf("txn%d", i)
		txnType := fmt.Sprintf("typ%d", i)
		spanName := fmt.Sprintf("spn%d", i)

		sim.AddTransaction(TransactionAggregationKey{
			TransactionName: txnName,
			TransactionType: txnType,
		}, WithTransactionCount(200))
		sim.AddServiceTransaction(ServiceTransactionAggregationKey{
			TransactionType: txnType,
		}, WithTransactionCount(200))
		sim.AddSpan(SpanAggregationKey{
			SpanName: spanName,
		})
	}
	cmproto := tcm.GetProto()
	b.Cleanup(func() {
		cmproto.ReturnToVTPool()
	})
	b.ResetTimer()
	var expected CombinedMetrics
	for i := 0; i < b.N; i++ {
		expected.FromProto(cmproto)
	}
}

func EncodeToCombinedMetricsKeyID(tb testing.TB, s string) [16]byte {
	var b [16]byte
	if len(s) > len(b) {
		tb.Fatal("invalid key length passed")
	}
	copy(b[len(b)-len(s):], s)
	return b
}
