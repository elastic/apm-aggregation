// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package hdrhistogram

import (
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
	expectedSnap := hist1.Export()

	assert.Empty(t, cmp.Diff(expectedSnap, histRep1.getHDRSnapshot()))
}

func getTestHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(
		lowestTrackableValue,
		highestTrackableValue,
		int(significantFigures),
	)
}
