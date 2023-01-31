package writepath

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	memtable "NAiSP/Structures/Memtable"
	record "NAiSP/Structures/Record"
	wal "NAiSP/Structures/WAL"
	"fmt"
)

// Store all types
type WritePath struct {
	Wal         *wal.WAL
	MemTable    *memtable.MemTable
	BloomFilter *bloomfilter.BloomFilter
}

// Write Path
func (wp *WritePath) Write(record *record.Record) {

	// First write in WAL
	writtenInWal := wp.Wal.AddRecordBuffered(record)

	if !writtenInWal {
		// Failed to write in WAL
		fmt.Println("Neuspesan upis u WAL. ")
		return
	}

	// If WAL has written record, then write in MemTable
	writtenInMem := wp.MemTable.Add(record)
	// If nill - not flushed
	if writtenInMem != nil {
		// TODO

		// Form new SsTable
		// Check for compaction
		return
	}

	// Add to bloom if not deleted
	if record.GetTombStone() == 0 {
		wp.BloomFilter.Hash(record.GetKey())
	}

}
