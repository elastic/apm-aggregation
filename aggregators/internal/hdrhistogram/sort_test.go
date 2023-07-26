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
