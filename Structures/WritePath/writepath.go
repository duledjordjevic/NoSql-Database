package writepath

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	memtable "NAiSP/Structures/Memtable"
	sstable "NAiSP/Structures/Sstable"
	wal "NAiSP/Structures/WAL"
	record "NAiSP/Structures/record"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
)

const (
	DIRECTORY  = "./Data/Data/"
	L0         = "/l0"
	COMPACTION = "Leveled/"
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
		SStable := sstable.NewSStableAutomatic(DIRECTORY+COMPACTION+"l0/", GenerateFileName("leveled"))
		SStable.FormSStable(writtenInMem)
		// Form new SsTable
		// Check for compaction
		return
	}

	// Add to bloom if not deleted
	if record.GetTombStone() == 0 {
		wp.BloomFilter.Hash(record.GetKey())
	}

}

func GenerateFileName(directory string) string {
	files, err := ioutil.ReadDir(DIRECTORY + directory + L0)
	if err != nil {
		fmt.Println("Greska kod citanja direktorijuma: ", err)
		log.Fatal(err)
	}

	lastFileName := files[len(files)-1]
	newFileName, err := strconv.Atoi(strings.Split(strings.Split(lastFileName.Name(), "_")[2], ".bin")[0])
	newFileName += 1

	return "_l0_" + strconv.FormatInt(int64(newFileName), 10)
}
