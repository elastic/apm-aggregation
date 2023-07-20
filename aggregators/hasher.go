// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"

	"github.com/elastic/apm-aggregation/aggregationpb"
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

func serviceKeyHasher(
	k *aggregationpb.ServiceAggregationKey,
) Hashable {
	return HashableFunc(func(h xxhash.Digest) xxhash.Digest {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], k.Timestamp)
		h.Write(buf[:])

		h.WriteString(k.ServiceName)
		h.WriteString(k.ServiceEnvironment)
		h.WriteString(k.ServiceLanguageName)
		h.WriteString(k.AgentName)
		return h
	})
}

func serviceInstanceKeyHasher(
	k *aggregationpb.ServiceInstanceAggregationKey,
) Hashable {
	return HashableFunc(func(h xxhash.Digest) xxhash.Digest {
		h.Write(k.GlobalLabelsStr)
		return h
	})
}

func serviceTransactionKeyHasher(
	k *aggregationpb.ServiceTransactionAggregationKey,
) Hashable {
	return HashableFunc(func(h xxhash.Digest) xxhash.Digest {
		h.WriteString(k.TransactionType)
		return h
	})
}

func spanKeyHasher(
	k *aggregationpb.SpanAggregationKey,
) Hashable {
	return HashableFunc(func(h xxhash.Digest) xxhash.Digest {
		h.WriteString(k.SpanName)
		h.WriteString(k.Outcome)

		h.WriteString(k.TargetType)
		h.WriteString(k.TargetName)

		h.WriteString(k.Resource)
		return h
	})
}

func transactionKeyHasher(
	k *aggregationpb.TransactionAggregationKey,
) Hashable {
	return HashableFunc(func(h xxhash.Digest) xxhash.Digest {
		if k.TraceRoot {
			h.WriteString("1")
		}

		h.WriteString(k.ContainerId)
		h.WriteString(k.KubernetesPodName)

		h.WriteString(k.ServiceVersion)
		h.WriteString(k.ServiceNodeName)

		h.WriteString(k.ServiceRuntimeName)
		h.WriteString(k.ServiceRuntimeVersion)
		h.WriteString(k.ServiceLanguageVersion)

		h.WriteString(k.HostHostname)
		h.WriteString(k.HostName)
		h.WriteString(k.HostOsPlatform)

		h.WriteString(k.EventOutcome)

		h.WriteString(k.TransactionName)
		h.WriteString(k.TransactionType)
		h.WriteString(k.TransactionResult)

		if k.FaasColdstart == uint32(True) {
			h.WriteString("1")
		}
		h.WriteString(k.FaasId)
		h.WriteString(k.FaasName)
		h.WriteString(k.FaasVersion)
		h.WriteString(k.FaasTriggerType)

		h.WriteString(k.CloudProvider)
		h.WriteString(k.CloudRegion)
		h.WriteString(k.CloudAvailabilityZone)
		h.WriteString(k.CloudServiceName)
		h.WriteString(k.CloudAccountId)
		h.WriteString(k.CloudAccountName)
		h.WriteString(k.CloudMachineType)
		h.WriteString(k.CloudProjectId)
		h.WriteString(k.CloudProjectName)
		return h
	})
}
