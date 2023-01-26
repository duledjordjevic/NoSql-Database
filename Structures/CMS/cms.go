package CMS

import (
	"encoding/gob"
	"os"
)

type CountMinSketch struct {
	Matrix [][]uint8
	Funcs  []HashWithSeed
}

func NewCountMinSketch(epsilon float64, delta float64) *CountMinSketch {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	return &CountMinSketch{Matrix: *MakeMatrix(k, m), Funcs: make([]HashWithSeed, k)}
}

func (cms *CountMinSketch) Add(i, j uint64) {
	cms.Matrix[i][j]++
}

func (cms *CountMinSketch) Hash(key string) {
	var index uint64
	for i := 0; i < len(cms.Funcs); i++ {
		index = cms.Funcs[i].Hash([]byte(key)) % uint64(len(cms.Matrix[i]))
		cms.Add(uint64(i), index)
	}
}

func (cms *CountMinSketch) Get(key string) uint8 {
	var index uint64
	var slice []uint8
	for i := 0; i < len(cms.Funcs); i++ {
		index = cms.Funcs[i].Hash([]byte(key)) % uint64(len(cms.Matrix[i]))
		slice = append(slice, cms.Matrix[i][index])
	}
	return Min(slice)
}

func (cms *CountMinSketch) Encode(fname string) {
	file, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	encoder := gob.NewEncoder(file)
	err2 := encoder.Encode(cms)
	if err2 != nil {
		panic(err2)
	}
	file.Close()
}

func (cms *CountMinSketch) Decode(fname string) {
	file, err := os.OpenFile(fname, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	decoder := gob.NewDecoder(file)
	err2 := decoder.Decode(&cms)
	if err2 != nil {
		panic(err2)
	}
	file.Close()
}
