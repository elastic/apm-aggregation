// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func BenchmarkGetBatchSerial(b *testing.B) {
	db, err := pebble.Open(b.TempDir(), nil)
	require.NoError(b, err)
	maxSize := 100
	bg := newBatchGroup(maxSize, db)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch, err := bg.getBatch()
		if err != nil {
			b.Fatalf("failed with err: %+v", err)
		}
		bg.releaseBatch(batch)
	}
}

func BenchmarkGetBatchParallel(b *testing.B) {
	db, err := pebble.Open(b.TempDir(), nil)
	require.NoError(b, err)
	maxSize := 100
	bg := newBatchGroup(maxSize, db)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			batch, err := bg.getBatch()
			if err != nil {
				b.Fatalf("failed with err: %+v", err)
			}
			bg.releaseBatch(batch)
		}
	})
}
