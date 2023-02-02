package HLL

import (
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	M         uint64
	P         uint8
	Registers []uint8
}

// procena kardinalnosti
func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Registers {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()

	if estimation <= 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.Registers {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func CreateHLL(p uint8) *HLL {
	if p > HLL_MAX_PRECISION || p < HLL_MIN_PRECISION {
		panic("Doslo je do greske. Broj p mora biti u opsegu : [4,16]")
	}
	hll := HLL{}
	hll.P = p
	hll.M = uint64(math.Pow(2, float64(hll.P)))
	hll.Registers = make([]uint8, hll.M, hll.M)

	return &hll
}

// func (hll *HLL) createHashFunction() {

// }

func (hll *HLL) AddElement(element string) {

	h := fnv.New32()
	_, err := h.Write([]byte(element))
	if err != nil {
		panic(err)
	}

	bytes := h.Sum32()

	index := bytes >> (32 - hll.P)
	endZeros := bits.TrailingZeros32(bytes)

	if hll.Registers[index] < uint8(endZeros) {
		hll.Registers[index] = uint8(endZeros)
	}
}

// func main() {

// 	hll := CreateHLL(6)

// 	hll.addElement("string")
// 	hll.addElement("string")
// 	hll.addElement("string")
// 	hll.addElement("string")
// 	hll.addElement("string")
// 	hll.addElement("string")
// 	hll.addElement("string")
// 	hll.addElement("text")
// 	hll.addElement("text1")
// 	// hll.addElement("1")
// 	// hll.addElement("asdjoiasdj")

// 	fmt.Println(hll.Estimate())
// 	fmt.Println(hll.Registers)
// }
