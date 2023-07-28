// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package hdrhistogram

import (
	"fmt"
	"sort"
)

func Example() {
	x := []int{3, 2, 1}
	y := []string{"a", "b", "c"}
	sort.Sort(SortBy[int, string]{x, y})
	fmt.Println(x, y)
	// Output:
	// [1 2 3] [c b a]
}
