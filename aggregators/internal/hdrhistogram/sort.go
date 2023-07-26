package hdrhistogram

import "golang.org/x/exp/constraints"

// SortBy sorts 2 slices together. SortBy.By will be sorted and corresponding
// swaps will happen in SortBy.Other.
type SortBy[T constraints.Ordered, T2 any] struct {
	By    []T
	Other []T2
}

func (s SortBy[T, T2]) Len() int {
	return len(s.By)
}

func (s SortBy[T, T2]) Swap(i, j int) {
	s.By[i], s.By[j] = s.By[j], s.By[i]
	s.Other[i], s.Other[j] = s.Other[j], s.Other[i]
}

func (s SortBy[T, T2]) Less(i, j int) bool {
	return s.By[i] < s.By[j]
}
