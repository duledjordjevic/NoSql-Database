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

	if lsm.Config.DataFileStructure == "Single" {
		// Read header
		// Seek on data part
	}

	return file
}
