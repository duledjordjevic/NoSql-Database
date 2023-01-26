package Bloomfilter

import (
	"encoding/gob"
	"os"
)

type BloomFilter struct {
	Arr   []uint8
	Funcs []HashWithSeed
}

func NewBLoomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	return &BloomFilter{Arr: MakeArr(m), Funcs: CreateHashFunctions(k)}
}

func (bf *BloomFilter) Position(index uint64) (uint64, uint8) {
	index = index % (uint64(len(bf.Arr)) * 8)
	bucket := index / 8
	mask := uint8(1) << ((index - 1) % 8)
	return uint64(len(bf.Arr)) - 1 - bucket, mask
}

func (bf *BloomFilter) Check(index uint64) bool {
	bucket, mask := bf.Position(index)
	return bf.Arr[bucket]&mask>>((index-1)%8) == 1
}

func (bf *BloomFilter) Set(index uint64) {
	if !bf.Check(index) {
		bucket, mask := bf.Position(index)
		bf.Arr[bucket] |= mask
	}
}

func (bf *BloomFilter) Hash(key string) {
	var index uint64
	for _, f := range bf.Funcs {
		index = f.Hash([]byte(key))
		bf.Set(index)
	}
}

func (bf *BloomFilter) Find(key string) bool {
	var index uint64
	for _, f := range bf.Funcs {
		index = f.Hash([]byte(key))
		if !bf.Check(index) {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Encode(fname string) {
	file, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	encoder := gob.NewEncoder(file)
	err2 := encoder.Encode(bf)
	if err2 != nil {
		panic(err2)
	}
	file.Close()
}

func (bf *BloomFilter) Decode(fname string) {
	file, err := os.OpenFile(fname, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	decoder := gob.NewDecoder(file)
	err2 := decoder.Decode(&bf)
	if err2 != nil {
		panic(err2)
	}
	file.Close()

}
