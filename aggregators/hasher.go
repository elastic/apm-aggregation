// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import "github.com/cespare/xxhash/v2"

type Hashable interface {
	Hash(xxhash.Digest) xxhash.Digest
}

type Hasher struct {
	digest xxhash.Digest // xxhash.Digest does not contain pointers and is safe to copy
}

func (h Hasher) Chain(hashable Hashable) Hasher {
	return Hasher{digest: hashable.Hash(h.digest)}
}

func (h Hasher) Sum() uint64 {
	return h.digest.Sum64()
}
