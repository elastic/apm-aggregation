// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package hdrhistogram

import (
	"math"
	"math/rand"
	"testing"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMerge(t *testing.T) {
	hist1, hist2 := getTestHistogram(), getTestHistogram()
	histRep1, histRep2 := New(), New()

	for i := 0; i < 1_000_000; i++ {
		v1, v2 := rand.Int63n(3_600_000_000), rand.Int63n(3_600_000_000)
		hist1.RecordValues(v1, 11)
		histRep1.RecordValues(v1, 11)
		hist2.RecordValues(v2, 111)
		histRep2.RecordValues(v2, 111)
	}

	require.Equal(t, int64(0), hist1.Merge(hist2))
	histRep1.Merge(histRep2)
	assert.Empty(t, cmp.Diff(hist1.Export(), convertHistogramRepToSnapshot(histRep1)))
}

func TestBuckets(t *testing.T) {
	buckets := func(h *hdrhistogram.Histogram) (int64, []int64, []float64) {
		distribution := h.Distribution()
		counts := make([]int64, 0, len(distribution))
		values := make([]float64, 0, len(distribution))

		var totalCount int64
		for _, b := range distribution {
			if b.Count <= 0 {
				continue
			}
			count := int64(math.Round(float64(b.Count) / histogramCountScale))
			counts = append(counts, count)
			values = append(values, float64(b.To))
			totalCount += count
		}
		return totalCount, counts, values
	}
	hist := getTestHistogram()
	histRep := New()

	for i := 0; i < 1_000_000; i++ {
		v := rand.Int63n(3_600_000_000)
		c := rand.Int63n(1_000)
		hist.RecordValues(v, c)
		histRep.RecordValues(v, c)
	}
	actualTotalCount, actualCounts, actualValues := histRep.Buckets()
	expectedTotalCount, expectedCounts, expectedValues := buckets(hist)

	assert.Equal(t, expectedTotalCount, actualTotalCount)
	assert.Equal(t, expectedCounts, actualCounts)
	assert.Equal(t, expectedValues, actualValues)
}

func getTestHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(
		lowestTrackableValue,
		highestTrackableValue,
		int(significantFigures),
	)
}

func convertHistogramRepToSnapshot(h *HistogramRepresentation) *hdrhistogram.Snapshot {
	counts := make([]int64, countsLen)
	for b, n := range h.CountsRep {
		counts[b] += n
	}
	return &hdrhistogram.Snapshot{
		LowestTrackableValue:  h.LowestTrackableValue,
		HighestTrackableValue: h.HighestTrackableValue,
		SignificantFigures:    h.SignificantFigures,
		Counts:                counts,
	}
}
