package writepath

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	memtable "NAiSP/Structures/Memtable"
	record "NAiSP/Structures/Record"
	sstable "NAiSP/Structures/Sstable"
	wal "NAiSP/Structures/WAL"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
)

const (
	DIRECTORY         = "./Data/Data/"
	L0                = "/l0"
	COMPACTIONleveled = "Leveled/"
	COMPACTIONtiered  = "Size_tiered"
)

// Store all types
type WritePath struct {
	Wal         *wal.WAL
	MemTable    *memtable.MemTable
	BloomFilter *bloomfilter.BloomFilter
	// Conifig     *configreader.ConfigReader
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
		// for leveled
		// if wp.Conifig.Compaction == "leveled" {
		// Generating new SSTable using next file suffix
		SStable := sstable.NewSStableAutomatic(DIRECTORY+COMPACTIONleveled+"l0/", GenerateFileName("leveled"))
		// Writting all data to disc
		SStable.FormSStable(writtenInMem)
		return
		// }
		// for size-tiered
		// SStable := sstable.NewSStableAutomatic(DIRECTORY+COMPACTIONtiered+"l0/", GenerateFileName("size_tiered"))
		// SStable.FormSStable(writtenInMem)
		return
	}

	// Add to bloom if not deleted
	wp.BloomFilter.Encode("./Data/Globalfilter/bloomfilter.gob")
	if record.GetTombStone() == 0 {
		fmt.Println(record.GetKey())
		wp.BloomFilter.Hash(record.GetKey())
	}
	wp.BloomFilter.Decode("./Data/Globalfilter/bloomfilter.gob")

}

func GenerateFileName(directory string) string {
	// Opening directory that contains data files
	files, err := ioutil.ReadDir(DIRECTORY + directory + L0)
	if err != nil {
		fmt.Println("Greska kod citanja direktorijuma: ", err)
		log.Fatal(err)
	}

	// Last file from level 0 -> largest index
	lastFileName := files[len(files)-1]
	newFileName, err := strconv.Atoi(strings.Split(strings.Split(lastFileName.Name(), "_")[2], ".bin")[0])
	newFileName += 1

	// Returns file suffix containing level and index
	return "_l0_" + strconv.FormatInt(int64(newFileName), 10)
}
