package types

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	"NAiSP/Structures/CMS"
	"NAiSP/Structures/HLL"
	simhash "NAiSP/Structures/Simhash"
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

func AddCMS(epsilon, delta float64) []byte {
	cms := CMS.NewCountMinSketch(epsilon, delta)
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(cms)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

func ReadCMS(s []byte) *CMS.CountMinSketch {
	cms := CMS.CountMinSketch{}
	dec := gob.NewDecoder(bytes.NewReader(s))
	err := dec.Decode(&cms)
	if err != nil {
		log.Fatal(err)
	}

	return &cms
}

func AppendElementCMS(element string, s []byte) []byte {
	cms := ReadCMS(s)
	cms.Hash(element)
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(cms)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

func GetElementRepetitionsCMS(element string, s []byte) uint8 {
	cms := ReadCMS(s)
	return cms.Get(element)
}

func AddHLL(p uint8) []byte {
	hll := HLL.CreateHLL(p)
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(hll)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

func ReadHLL(s []byte) *HLL.HLL {
	hll := HLL.HLL{}
	dec := gob.NewDecoder(bytes.NewReader(s))
	err := dec.Decode(&hll)
	if err != nil {
		log.Fatal(err)
	}

	return &hll
}

func AppendElementHLL(element string, s []byte) []byte {
	hll := ReadHLL(s)
	hll.AddElement(element)
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(hll)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

func CheckCardinalityHLL(s []byte) float64 {
	hll := ReadHLL(s)

	return hll.Estimate()
}

func AddSimHash(value string) []byte {
	simHash := simhash.CreateSimHash(value)
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(simHash)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

func ReadSimHash(s []byte) *simhash.SimHash {
	simHash := simhash.SimHash{}
	dec := gob.NewDecoder(bytes.NewReader(s))
	err := dec.Decode(&simHash)
	if err != nil {
		log.Fatal(err)
	}

	return &simHash
}

func CompareSimHash(value string, s []byte) string {
	simHash := ReadSimHash(s)

	return simHash.Comparation(value)
}
