package types

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	"bytes"
	"encoding/gob"
	"log"
)

func AddBloomFilter(expectedElements int, falsePositiveRate float64) []byte {
	bf := bloomfilter.NewBLoomFilter(expectedElements, falsePositiveRate)
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(bf)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

func ReadBloomFilter(s []byte) *bloomfilter.BloomFilter {
	bf := bloomfilter.BloomFilter{}
	dec := gob.NewDecoder(bytes.NewReader(s))
	err := dec.Decode(&bf)
	if err != nil {
		log.Fatal(err)
	}

	return &bf
}

func AppendElementBloomFilter(element string, s []byte) []byte {
	bf := ReadBloomFilter(s)
	bf.Hash(element)
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(bf)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

func CheckElementBloomFilter(element string, s []byte) bool {
	bf := ReadBloomFilter(s)

	return bf.Find(element)
}

// func AddCMS(epsilon, delta float64) []byte {

// }
