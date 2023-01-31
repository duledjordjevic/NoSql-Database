package readpath

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	lru "NAiSP/Structures/LRUcache"
	memtable "NAiSP/Structures/Memtable"
)

type ReadPath struct {
	MemTable    *memtable.MemTable
	Lru         *lru.LRUCache
	BloomFilter *bloomfilter.BloomFilter
	// Need SsTable
}

func (rp *ReadPath) Read(key string) []byte {

	// First check in MemTable
	record := rp.MemTable.Find(key)
	if record != nil {
		rp.Lru.AddElement(record.GetKey(), record.GetValue())
		return record.GetValue()
	}

	// Next check in Cache
	value := rp.Lru.GetElement(key)
	if value != nil {
		return value
	}

	// Next check Bloom Filter
	found := rp.BloomFilter.Find(key)
	if !found {
		// If not in bloom we can be sure it's not there
		return nil
	}

	// TODO

	// find valid summary
	return nil
}
