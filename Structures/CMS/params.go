package CMS

import (
	"math"
)

func CalculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func CalculateK(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

func MakeMatrix(k, m uint) *[][]uint8 {
	matrix := make([][]uint8, k)
	for i := range matrix {
		matrix[i] = make([]uint8, m)
	}
	return &matrix
}

func Min(array []uint8) uint8 {
	min := uint8(math.MaxInt8)
	for _, el := range array {
		if el < min {
			min = el
		}
	}
	return min
}
