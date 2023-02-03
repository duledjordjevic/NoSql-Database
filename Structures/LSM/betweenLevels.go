package lsm

import (
	record "NAiSP/Structures/Record"
	sstable "NAiSP/Structures/Sstable"
	"fmt"
	"math"
	"os"
)

func (lvl *Leveled) BetweenLevels(from int, to int) {
	firstLevel := lvl.levels[from][:int64(math.Pow(CAPACITY, float64(from)))]
	secondLevel := lvl.levels[to]

	iteratorFirst := 0
	iteratorSecond := 0

	// oppening the first file from each level
	first := lvl.lsm.OpenData(firstLevel[0])
	second := lvl.lsm.OpenData(secondLevel[0])

	// every newly generated sstable will be added to this list
	tempSSTables := make([]*sstable.SStable, 0)

	// file counters for levels
	counterFirst := len(firstLevel)
	counterSecond := len(secondLevel)

	// SSTables for reading header
	SSTableFirst := lvl.NewSSTableFromFileName(first)
	SSTableSecond := lvl.NewSSTableFromFileName(second)

	SSTable := lvl.NewSSTable(0, to, true)
	files, writers, counter, offsetData, offsetIndex, bf, merkle := lvl.InitSSTable(SSTable)
	tempSSTables = append(tempSSTables, SSTable)

	// necessary for generating filename
	fileCounter := 0

	// necessary for checking if file is full
	currentCapacity := 0

	fromTo := make(map[*os.File][]int)
	beggining := true

	// data file values
	var recordFirst *record.Record
	var recordSecond *record.Record

	// header values
	var firstHeaderRecord *record.Record
	var lastHeaderRecord *record.Record

	for {
		// level first got empied -> rename the rest of level two
		if counterFirst == 0 && counterSecond != 0 {
			// terminal condition 1

			// // empty the remaining first file
			// lvl.EmptyFile(first, SSTable, currentCapacity, firstRecord, lastRecord, files, writers,
			// 	bf, merkle, fileCounter, counter, offsetData, offsetIndex, tempSSTables)

			// // closing the last SSTable
			// SSTable.CopyExistingToSummary(firstRecord, lastRecord, files, writers)
			// SSTable.EncodeHelpers(bf, merkle)
			// SSTable.CloseFiles(files)

			// // renaming reamining files to temp files

			// if iterator+1 <= len(lvl.levels[1])-1 {
			// 	for _, firstFile := range lvl.levels[1][iterator+1:] {
			// 		// rename element
			// 		remainingSSTable := lvl.RenameFile(fileCounter, firstFile)
			// 		fileCounter++

			// 		// append renamed to temp
			// 		tempSSTables = append(tempSSTables, remainingSSTable)

			// 	}
			// }

			// // renaming temo files
			// lvl.RenameLevel(tempSSTables)

			// lvl.GenerateLevels()
			// // calls between levels
			// if len(tempSSTables) > CAPACITY {
			// 	lvl.BetweenLevels()
			// }

		}
		// level second got emptied first -> rename the rest of level one
		if counterFirst != 0 && counterSecond == 0 {
			// terminal condition 2

			// same as terminal condition 1

		}

		if beggining {
			if fromTo[first][0] < fromTo[second][0] && fromTo[first][1] < fromTo[second][0] {
				// renaming file from first level
				// open next file from first level
				// counterFirst --
				// read header
				// update map fromTo with new values
				// continue
			} else if fromTo[second][0] < fromTo[first][0] && fromTo[second][1] < fromTo[first][0] {
				// renaming file from second level
				// open next file from second level
				// counterSecond --
				// read header
				// update map fromTo with new values
				// continue
			} else {
				beggining = false
			}
		}

		// sequential processing of both files
		minimumFile, minimumRecord := lvl.GetMinimumRecord(recordFirst, first, recordSecond, second)

		currentCapacity += int(minimumRecord.GetSize())

		// if current sstable reached capacity -> make a new one
		if currentCapacity > SSTABLE_CAPACITY {

			// completing formation of SSTable
			SSTable.CopyExistingToSummary(firstHeaderRecord, lastHeaderRecord, files, writers)
			SSTable.EncodeHelpers(bf, merkle)
			SSTable.CloseFiles(files)
			// and closing it

			// Initialiazing new SSTABLE -> adding last record to new SSTABLE
			fileCounter++
			SSTable = lvl.NewSSTable(fileCounter, 1, true)
			currentCapacity = int(minimumRecord.GetSize())
			// initializing all necesarry files
			files, writers, counter, offsetData, offsetIndex, bf, merkle = lvl.InitSSTable(SSTable)

			tempSSTables = append(tempSSTables, SSTable)

			firstHeaderRecord = minimumRecord

			// add record
			offsetData, offsetIndex = SSTable.AddRecord(counter, offsetData, offsetIndex, minimumRecord, bf, merkle, writers)
			counter++
			// TODO
		} else {
			// add record
			if firstHeaderRecord == nil {
				firstHeaderRecord = minimumRecord
			}

			offsetData, offsetIndex = SSTable.AddRecord(counter, offsetData, offsetIndex, minimumRecord, bf, merkle, writers)
			counter++
			// TODO
		}

		fmt.Println("AFTER WRITING TO SSTABLE -> ", minimumRecord)

		// remembers the last record -> will be necessary in header later
		lastHeaderRecord = minimumRecord

		// read next record from minimum
		// minimumFile = lvl.NextRecord(minimumFile, first, &counter0, &counter1, &iterator)

		minimumRecord, _ = record.ReadRecord(minimumFile)
		if minimumRecord == nil {
			if minimumFile == first {
				counterFirst--

			} else {
				counterSecond--
			}
		}

		fmt.Println("AFTER NEXT RECORD -> ", minimumRecord)

	}

}

func (lvl *Leveled) GetMinimumRecord(recordFirst *record.Record, first *os.File, recordSecond *record.Record, second *os.File) (*os.File, *record.Record) {
	// initialiazing values for first and second record

	if recordFirst == nil {
		recordFirst, _ = record.ReadRecord(first)
	}
	if recordSecond == nil {
		recordSecond, _ = record.ReadRecord(second)
	}

	// comparing first and second records
	if recordFirst.GetKey() > recordSecond.GetKey() {
		return first, recordFirst

	} else if recordFirst.GetKey() < recordSecond.GetKey() {
		return second, recordSecond

	} else {
		if recordFirst.GetTimeStamp() > recordSecond.GetTimeStamp() {
			return first, recordFirst
		} else {
			return second, recordSecond
		}
	}
	return nil, nil
}

func (lvl *Leveled) NextRecordBetweenLevels(minimumFile *os.File, first *os.File,
	second *os.File, counterFirst *int, counterSecond *int, iteratorFirst *int, iteratorSecond *int, firstLevel *[]string, secondLevel *[]string) *record.Record {

	nextRecord, _ := record.ReadRecord(minimumFile)
	if nextRecord == nil {
		// deleting the file and removing it from the map
		if minimumFile == first {
			*counterFirst--
			*iteratorFirst++

			if *iteratorFirst > len(*firstLevel)-1 {
				first = nil
			} else {
				first = lvl.lsm.OpenData((*firstLevel)[*iteratorFirst])

			}
		} else {
			*counterSecond--
		}
	}
}
