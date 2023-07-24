// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

// HashPartitioner is a hash based partitioner for pebble keys. The paritioner
// partitions the keys into buckets with number of buckets limited by the passed
// argument.
type HashPartitioner struct {
	maxPartitions uint16
}

// NewHashPartitioner creates a new instance of the HashPartitioner.
func NewHashPartitioner(maxPartitions uint16) *HashPartitioner {
	return &HashPartitioner{
		maxPartitions: maxPartitions,
	}
}

// Partition generates an ID to be used as partition ID given a hash of the key.
func (p *HashPartitioner) Partition(hasher Hasher) uint16 {
	return uint16(hasher.Sum() % uint64(p.maxPartitions))
}
