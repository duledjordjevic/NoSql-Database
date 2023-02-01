package writepath

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	configreader "NAiSP/Structures/ConfigReader"
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
	DIRECTORY   = "./Data/Data"
	BLOOMFILTER = "/bloomfilter.gob"
)

// Store all types
type WritePath struct {
	Wal         *wal.WAL
	MemTable    *memtable.MemTable
	BloomFilter *bloomfilter.BloomFilter
	Config      *configreader.ConfigReader
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
	// If nill -> not flushed
	if writtenInMem != nil {
		// Generating new SSTable using next file suffix
		directory := DIRECTORY + wp.Config.DataFileStructure + "/" + wp.Config.Compaction + "/Data"

		SStable := sstable.NewSStableAutomatic(GenerateSufix(directory, 0), wp.Config)
		// Writting all data to disc

		SStable.FormSStableTest(writtenInMem)
		return

	}

	wp.BloomFilter.Hash(record.GetKey())

	// Add to bloom if not deleted
	// wp.BloomFilter.Encode(DIRECTORY + wp.Config.DataFileStructure + "/" + wp.Config.Compaction + BLOOMFILTER)
	// if record.GetTombStone() == 0 {
	// 	wp.BloomFilter.Hash(record.GetKey())
	// }
	// wp.BloomFilter.Decode(DIRECTORY + wp.Config.DataFileStructure + "/" + wp.Config.Compaction + BLOOMFILTER)

}

func GenerateSufix(directory string, level int) string {
	// Opening directory that contains data files
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		fmt.Println("Greska kod citanja direktorijuma - GenerateFileName: ", err)
		log.Fatal(err)
	}

	fmt.Println("Duzina -> ", len(files))

	if len(files) == 0 {
		return "_l0_0"
	}

	last := ""
	for _, file := range files {
		if GetLevel(file.Name()) == level {
			last = file.Name()
			continue
		}

		// if level < getLevel(file.Name()) && last == "" {
		// 	return "_l" + strconv.FormatInt(int64(level), 10) + "_0"
		// }

		if GetLevel(file.Name()) > level {
			break
		}
	}

	if last == "" {
		return "_l" + strconv.FormatInt(int64(level), 10) + "_0"
	}

	newFileName, err := strconv.Atoi(strings.Split(strings.Split(last, "_")[2], ".bin")[0])
	newFileName += 1

	// Returns file suffix containing level and index
	return "_l" + strconv.FormatInt(int64(level), 10) + "_" + strconv.FormatInt(int64(newFileName), 10)
}

func GetLevel(filename string) int {
	level, _ := strconv.Atoi(strings.Split(strings.Split(filename, "_")[1], "l")[1])
	return level
}
