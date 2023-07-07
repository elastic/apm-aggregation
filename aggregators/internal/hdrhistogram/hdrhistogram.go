// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package hdrhistogram provides an optimized histogram for sparse samples.
// This is a stop gap measure until we have [packed histogram implementation](https://www.javadoc.io/static/org.hdrhistogram/HdrHistogram/2.1.12/org/HdrHistogram/PackedHistogram.html).
package hdrhistogram

import (
	"fmt"
	"math"
	"math/bits"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

const (
	lowestTrackableValue  = 1
	highestTrackableValue = 3.6e+9 // 1 hour in microseconds
	significantFigures    = 2

	// We scale transaction counts in the histogram, which only permits storing
	// integer counts, to allow for fractional transactions due to sampling.
	//
	// e.g. if the sampling rate is 0.4, then each sampled transaction has a
	// representative count of 2.5 (1/0.4). If we receive two such transactions
	// we will record a count of 5000 (2 * 2.5 * histogramCountScale). When we
	// publish metrics, we will scale down to 5 (5000 / histogramCountScale).
	histogramCountScale = 1000
)

var (
	unitMagnitude               = getUnitMagnitude()
	subBucketHalfCountMagnitude = getSubBucketHalfCountMagnitude()
	subBucketHalfCount          = getSubBucketHalfCount()
	subBucketMask               = getSubBucketMask()
	countsLen                   = getCountsLen()
)

// HistogramRepresentation is an optimization over HDR histogram mainly useful
// for recording values clustered in some range rather than distributed over
// the full range of the HDR histogram. It is based on the [hdrhistogram-go](https://github.com/HdrHistogram/hdrhistogram-go) package.
// The package is not safe for concurrent usage, use an external lock
// protection if required.
type HistogramRepresentation struct {
	LowestTrackableValue  int64
	HighestTrackableValue int64
	SignificantFigures    int64
	CountsRep             map[int32]int64
}

// New returns a new instance of HistogramRepresentation
func New() *HistogramRepresentation {
	return &HistogramRepresentation{
		LowestTrackableValue:  lowestTrackableValue,
		HighestTrackableValue: highestTrackableValue,
		SignificantFigures:    significantFigures,
		CountsRep:             make(map[int32]int64),
	}
}

// RecordDuration records duration in the histogram representation. It
// supports recording float64 upto 3 decimal places. This is achieved
// by scaling the count.
func (h *HistogramRepresentation) RecordDuration(d time.Duration, n float64) error {
	count := int64(math.Round(n * histogramCountScale))
	v := d.Microseconds()

	return h.RecordValues(v, count)
}

// RecordValues records values in the histogram representation.
func (h *HistogramRepresentation) RecordValues(v, n int64) error {
	idx := h.countsIndexFor(v)
	if idx < 0 || int32(countsLen) <= idx {
		return fmt.Errorf("value %d is too large to be recorded", v)
	}
	h.CountsRep[idx] += n
	return nil
}

// Merge merges the provided histogram representation.
// TODO: Add support for migration from a histogram representation
// with different parameters.
func (h *HistogramRepresentation) Merge(from *HistogramRepresentation) {
	if from == nil {
		return
	}
	for b, n := range from.CountsRep {
		h.CountsRep[b] += n
	}
}

// Buckets converts the histogram into ordered slices of counts
// and values per bar along with the total count.
func (h *HistogramRepresentation) Buckets() (int64, []int64, []float64) {
	// TODO: This can be done without importing to hdr snapshot
	hist := hdrhistogram.Import(h.getHDRSnapshot())
	distribution := hist.Distribution()
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

// getHDRSnapshot returns the official hdrhistogram.Snapshot.
func (h *HistogramRepresentation) getHDRSnapshot() *hdrhistogram.Snapshot {
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

func (h *HistogramRepresentation) countsIndexFor(v int64) int32 {
	bucketIdx := h.getBucketIndex(v)
	subBucketIdx := h.getSubBucketIdx(v, bucketIdx)
	return h.countsIndex(bucketIdx, subBucketIdx)
}

func (h *HistogramRepresentation) countsIndex(bucketIdx, subBucketIdx int32) int32 {
	baseBucketIdx := (bucketIdx + 1) << uint(subBucketHalfCountMagnitude)
	return baseBucketIdx + subBucketIdx - subBucketHalfCount
}

func (h *HistogramRepresentation) getBucketIndex(v int64) int32 {
	var pow2Ceiling = int64(64 - bits.LeadingZeros64(uint64(v|subBucketMask)))
	return int32(pow2Ceiling - int64(unitMagnitude) -
		int64(subBucketHalfCountMagnitude+1))
}

func (h *HistogramRepresentation) getSubBucketIdx(v int64, idx int32) int32 {
	return int32(v >> uint(int64(idx)+int64(unitMagnitude)))
}

func getSubBucketHalfCountMagnitude() int32 {
	largetValueWithSingleUnitResolution := 2 * math.Pow10(significantFigures)
	subBucketCountMagnitude := int32(math.Ceil(math.Log2(
		largetValueWithSingleUnitResolution,
	)))
	if subBucketCountMagnitude < 1 {
		return 0
	}
	return subBucketCountMagnitude - 1
}

func getUnitMagnitude() int32 {
	unitMag := int32(math.Floor(math.Log2(
		lowestTrackableValue,
	)))
	if unitMag < 0 {
		return 0
	}
	return unitMag
}

func getSubBucketCount() int32 {
	return int32(math.Pow(2, float64(getSubBucketHalfCountMagnitude()+1)))
}

func getSubBucketHalfCount() int32 {
	return getSubBucketCount() / 2
}

func getSubBucketMask() int64 {
	return int64(getSubBucketCount()-1) << uint(getUnitMagnitude())
}

func getCountsLen() int64 {
	smallestUntrackableValue := int64(getSubBucketCount()) << uint(getUnitMagnitude())
	bucketsNeeded := int32(1)
	for smallestUntrackableValue < highestTrackableValue {
		if smallestUntrackableValue > (math.MaxInt64 / 2) {
			// next shift will overflow, meaning that bucket could
			// represent values up to ones greater than math.MaxInt64,
			// so it's the last bucket
			return int64(bucketsNeeded + 1)
		}
		smallestUntrackableValue <<= 1
		bucketsNeeded++
	}
	return int64((bucketsNeeded + 1) * (getSubBucketCount() / 2))
}
