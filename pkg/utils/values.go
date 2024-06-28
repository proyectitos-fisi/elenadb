package utils

import "golang.org/x/exp/constraints"

func Max[K constraints.Ordered](orderedSlice ...K) K {
	if len(orderedSlice) == 0 {
		panic("received empty arguments. Can't calculate max.")
	}
	v := orderedSlice[0]
	for i := 1; i < len(orderedSlice); i++ {
		v2 := orderedSlice[i]
		if v2 > v {
			v = v2
		}
	}
	return v
}

func Min[K constraints.Ordered](orderedSlice ...K) K {
	if len(orderedSlice) == 0 {
		panic("received empty arguments. Can't calculate min.")
	}
	v := orderedSlice[0]
	for i := 1; i < len(orderedSlice); i++ {
		v2 := orderedSlice[i]
		if v2 < v {
			v = v2
		}
	}
	return v
}
