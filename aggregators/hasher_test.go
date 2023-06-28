// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cespare/xxhash/v2"
)

type testHashable func(xxhash.Digest) xxhash.Digest

func (f testHashable) Hash(h xxhash.Digest) xxhash.Digest {
	return f(h)
}

func TestHasher(t *testing.T) {
	a := Hasher{}
	b := a.Chain(testHashable(func(h xxhash.Digest) xxhash.Digest {
		h.WriteString("1")
		return h
	}))
	c := a.Chain(testHashable(func(h xxhash.Digest) xxhash.Digest {
		h.WriteString("1")
		return h
	}))
	assert.NotEqual(t, a, b)
	assert.Equal(t, b, c)

	// Ensure the struct does not change after calling Sum
	c.Sum()
	assert.Equal(t, b, c)
}
