package utils

import "golang.org/x/exp/constraints"

// FLAG_ALGORITMO: Algoritmo: Búsqueda lineal del máximo
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

// FLAG_ALGORITMO: Algoritmo: Búsqueda lineal del min
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

func SafeSubtractUint16(a, b uint16) (uint16, bool) {
	// Check if subtracting b from a would cause an underflow
	if a < b {
		return 0, false // Indicate an overflow occurred
	}
	return a - b, true
}
