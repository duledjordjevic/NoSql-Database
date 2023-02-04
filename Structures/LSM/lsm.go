package lsm

import (
	configreader "NAiSP/Structures/ConfigReader"
	sstable "NAiSP/Structures/Sstable"
	"fmt"
	"os"
)

type LSM struct {
	Config  *configreader.ConfigReader
	ssTable *sstable.SStable
}

// Open file depending on DataFileStructure
func (lsm *LSM) OpenData(fileName string) *os.File {

	// Open file
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Error, ", err)
	}

	// Seek to start
	file.Seek(0, 0)

	ssTable := sstable.SStable{}
	if lsm.Config.DataFileStructure == "Single" {
		// Read header
		sizebloom, sizesummary, sizeindex := ssTable.ReadSStableHeader(file)
		file.Seek(int64(sizebloom)+int64(sizesummary)+int64(sizeindex), 1)
	}

	return file
}

func (lsm *LSM) ReadHeader(SSTable *sstable.SStable) (string, string) {
	file, err := os.Open(SSTable.SummaryPath)
	if err != nil {
		fmt.Println("Error, ", err)
	}

	file.Seek(0, 0)

	if lsm.Config.DataFileStructure == "Single" {
		bloomSize, _, _ := SSTable.ReadSStableHeader(file)
		file.Seek(int64(bloomSize), 1)
	}
	summaryHeader, _ := sstable.ReadSumarryHeader(file)

	return summaryHeader.GetKeyMin(), summaryHeader.GetKeyMax()
}
