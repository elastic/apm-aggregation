// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"github.com/cespare/xxhash/v2"
)

// HashableFunc is a function type that implements Hashable.
type HashableFunc func(xxhash.Digest) xxhash.Digest

// Hash calls HashableFunc function.
func (f HashableFunc) Hash(d xxhash.Digest) xxhash.Digest {
	return f(d)
}

// Hashable represents the hash function interface implemented by aggregation models.
type Hashable interface {
	Hash(xxhash.Digest) xxhash.Digest
}

// Hasher contains a safe to copy digest.
type Hasher struct {
	digest xxhash.Digest // xxhash.Digest does not contain pointers and is safe to copy
}

// Chain allows chaining hash functions for Hashable interfaces.
func (h Hasher) Chain(hashable Hashable) Hasher {
	return Hasher{digest: hashable.Hash(h.digest)}
}

// Sum returns the hash for all the chained interfaces.
func (h Hasher) Sum() uint64 {
	return h.digest.Sum64()
}
