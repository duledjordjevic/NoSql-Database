package writepath

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	configreader "NAiSP/Structures/ConfigReader"
	memtable "NAiSP/Structures/Memtable"
	record "NAiSP/Structures/Record"
	sstable "NAiSP/Structures/Sstable"
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
	Wal         *WAL
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

		if wp.Config.DataFileStructure == "Multiple" {
			directory := DIRECTORY + wp.Config.DataFileStructure + "/" + wp.Config.Compaction + "/Data"

			SStable := sstable.NewSStableAutomatic(GenerateSufix(directory, 0), wp.Config)
			SStable.FormSStableTest(writtenInMem)

		} else {
			directory := DIRECTORY + wp.Config.DataFileStructure + "/" + wp.Config.Compaction
			SStable := &sstable.SStable{
				SStableFilePath: directory + "/Data" + "/data" + GenerateSufix(directory+"/Data", 0) + ".bin",
				TOCFilePath:     directory + "/Toc" + "/TOC" + GenerateSufix(directory+"/Data", 0) + ".txt",
				MetaDataPath:    directory + "/Data" + "/Metadata" + GenerateSufix(directory+"/Data", 0) + ".txt"}

			SStable.FormSStableOneFile(writtenInMem)

		}

	}

	wp.BloomFilter.Hash(record.GetKey())

	// Add to bloom if not deleted
	// wp.BloomFilter.Encode(DIRECTORY + wp.Config.DataFileStructure + "/" + wp.Config.Compaction + BLOOMFILTER)
	// if record.GetTombStone() == 0 {
	// 	wp.BloomFilter.Hash(record.GetKey())
	// }
	// wp.BloomFilter.Decode(DIRECTORY + wp.Config.DataFileStructure + "/" + wp.Config.Compaction + BLOOMFILTER)

}

// Reconstruction
func (wp *WritePath) Reconstruction(record *record.Record) {

	// If WAL has written record, then write in MemTable
	writtenInMem := wp.MemTable.Add(record)
	// If nill -> not flushed
	if writtenInMem != nil {
		// Generating new SSTable using next file suffix

		if wp.Config.DataFileStructure == "Multiple" {
			directory := DIRECTORY + wp.Config.DataFileStructure + "/" + wp.Config.Compaction + "/Data"

			SStable := sstable.NewSStableAutomatic(GenerateSufix(directory, 0), wp.Config)
			SStable.FormSStableTest(writtenInMem)

		} else {
			directory := DIRECTORY + wp.Config.DataFileStructure + "/" + wp.Config.Compaction
			SStable := &sstable.SStable{
				SStableFilePath: directory + "/Data" + "/data" + GenerateSufix(directory+"/Data", 0) + ".bin",
				TOCFilePath:     directory + "/Toc" + "/TOC" + GenerateSufix(directory+"/Data", 0) + ".txt",
				MetaDataPath:    directory + "/Data" + "/Metadata" + GenerateSufix(directory+"/Data", 0) + ".txt"}

			SStable.FormSStableOneFile(writtenInMem)

		}

	}

	wp.BloomFilter.Hash(record.GetKey())

}

func GenerateSufix(directory string, level int) string {
	// Opening directory that contains data files
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		fmt.Println("Greska kod citanja direktorijuma - GenerateFileName: ", err)
		log.Fatal(err)
	}

	// fmt.Println("Duzina -> ", len(files))

	if len(files) == 0 {
		return "_l0_0"
	}

	last := "a_l0_0.bin"
	for _, file := range files {
		if GetLevel(file.Name()) == level {
			if GetIndex(file.Name()) >= GetIndex(last) {
				last = file.Name()

			}
			continue
		}

		if GetLevel(file.Name()) > level {
			break
		}
	}

	if last == "" {
		return "_l" + strconv.FormatInt(int64(level), 10) + "_0"
	}

	newFileName, _ := strconv.Atoi(strings.Split(strings.Split(last, "_")[2], ".")[0])
	newFileName++

	// Returns file suffix containing level and index
	return "_l" + strconv.FormatInt(int64(level), 10) + "_" + strconv.FormatInt(int64(newFileName), 10)
}

func GetLevel(filename string) int {
	level, _ := strconv.Atoi(strings.Split(strings.Split(filename, "_")[1], "l")[1])
	return level
}
func GetIndex(filename string) int {
	level, _ := strconv.Atoi(strings.Split(strings.Split(filename, "_")[2], ".")[0])
	return level
}

func (wp *WritePath) ExitFlush() {
	records := wp.MemTable.Flush()
	if len(*records) != 0 {
		// fmt.Println("usooooooooo")
		// fmt.Println(records)
		if wp.Config.DataFileStructure == "Multiple" {
			directory := DIRECTORY + wp.Config.DataFileStructure + "/" + wp.Config.Compaction + "/Data"

			SStable := sstable.NewSStableAutomatic(GenerateSufix(directory, 0), wp.Config)
			SStable.FormSStableTest(records)

		} else {
			directory := DIRECTORY + wp.Config.DataFileStructure + "/" + wp.Config.Compaction
			SStable := &sstable.SStable{
				SStableFilePath: directory + "/Data" + "/data" + GenerateSufix(directory+"/Data", 0) + ".bin",
				TOCFilePath:     directory + "/Toc" + "/TOC" + GenerateSufix(directory+"/Data", 0) + ".txt",
				MetaDataPath:    directory + "/Data" + "/Metadata" + GenerateSufix(directory+"/Data", 0) + ".txt"}

			SStable.FormSStableOneFile(records)
		}
		for _, rec := range *records {
			wp.BloomFilter.Hash(rec.GetKey())
		}

	}

}
