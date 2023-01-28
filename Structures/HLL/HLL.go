package HLL

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	m         uint64
	p         uint8
	registers []uint8
}

// procena kardinalnosti
func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.registers {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()

	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.registers {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func createHLL(p uint8) *HLL {
	if p > HLL_MAX_PRECISION || p < HLL_MIN_PRECISION {
		panic("Doslo je do greske. Broj p mora biti u opsegu : [4,16]")
	}
	hll := HLL{}
	hll.p = p
	hll.m = uint64(math.Pow(2, float64(hll.p)))
	hll.registers = make([]uint8, hll.m, hll.m)

	return &hll
}

// func (hll *HLL) createHashFunction() {

// }

func (hll *HLL) addElement(element string) {

	h := fnv.New32()
	_, err := h.Write([]byte(element))
	if err != nil {
		panic(err)
	}

	bytes := h.Sum32()

	index := bytes >> (32 - hll.p)
	endZeros := bits.TrailingZeros32(bytes)

	if hll.registers[index] < uint8(endZeros) {
		hll.registers[index] = uint8(endZeros)
	}
}

func main() {

	hll := createHLL(6)

	hll.addElement("string")
	hll.addElement("string")
	hll.addElement("string")
	hll.addElement("string")
	hll.addElement("string")
	hll.addElement("string")
	hll.addElement("string")
	hll.addElement("text")
	hll.addElement("text1")
	// hll.addElement("1")
	// hll.addElement("asdjoiasdj")

	fmt.Println(hll.Estimate())
	fmt.Println(hll.registers)
}
