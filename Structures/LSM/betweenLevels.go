package lsm

import (
	record "NAiSP/Structures/Record"
	sstable "NAiSP/Structures/Sstable"
	"fmt"
	"math"
	"os"
)

func (lvl *Leveled) calculateCapacity(level int) int {

	fmt.Println("PROCENAT -> ", int(math.Ceil(float64(len(lvl.levels[level]))-PERCENT*math.Pow(CAPACITY, float64(level)))))
	return int(math.Ceil(float64(len(lvl.levels[level])) - PERCENT*math.Pow(CAPACITY, float64(level))))
}

func (lvl *Leveled) BetweenLevels(from int, to int) {
	firstLevel := lvl.levels[from][:lvl.calculateCapacity(from)]
	secondLevel := lvl.levels[to]

	// TODO -> if second level is empty -> rename first level and break the loop

	iteratorFirst := 0
	iteratorSecond := 0

	// oppening the first file from each level
	first := lvl.lsm.OpenData(firstLevel[0])

	fmt.Println("FILENAME BETWEENLEVELS -> ", first.Name())

	var second *os.File

	if secondLevel != nil {
		second = lvl.lsm.OpenData(secondLevel[0])
	} else {
		second = nil
	}

	// every newly generated sstable will be added to this list
	tempSSTables := make([]*sstable.SStable, 0)

	// file counters for levels
	counterFirst := len(firstLevel)

	var counterSecond int

	if second != nil {
		counterSecond = len(secondLevel)
	} else {
		counterSecond = 0
	}

	// SSTables for reading header
	SSTableFirst := lvl.NewSSTableFromFileName(first)

	var SSTableSecond *sstable.SStable

	if second != nil {
		SSTableSecond = lvl.NewSSTableFromFileName(second)
	} else {
		SSTableSecond = nil
	}

	SSTable := lvl.NewSSTable(0, to, true)
	files, writers, counter, offsetData, offsetIndex, bf, merkle := lvl.InitSSTable(SSTable)
	tempSSTables = append(tempSSTables, SSTable)

	// necessary for generating filename
	fileCounter := 0

	// necessary for checking if file is full
	currentCapacity := 0

	lvl.fromTo = make(map[*os.File][]string)

	print("SUMMARY -> ", SSTableFirst.SummaryPath)

	lvl.fromTo[first] = lvl.lsm.ReadHeader(SSTableFirst)
	if second != nil {
		lvl.fromTo[second] = lvl.lsm.ReadHeader(SSTableSecond)
	}

	beginning := true

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

			// empty the remaining first file
			lvl.EmptyFile(second, SSTable, currentCapacity, firstHeaderRecord, lastHeaderRecord, files, writers,
				bf, merkle, fileCounter, counter, offsetData, offsetIndex, tempSSTables)

			fmt.Println("OVAJ KOJI SE PREPISUJE -> ", second.Name())

			// // closing the last file
			// fileName := second.Name()
			// second.Close()
			// err := os.Remove(fileName)
			// if err != nil {
			// 	fmt.Println("Brisanje ispraznjenog fajla")
			// }

			// closing the last SSTable
			SSTable.CopyExistingToSummary(firstHeaderRecord, lastHeaderRecord, files, writers)
			SSTable.EncodeHelpers(bf, merkle)
			SSTable.CloseFiles(files)

			// renaming reamining files to temp files

			if iteratorSecond+1 <= len(secondLevel)-1 {
				for _, file := range secondLevel[iteratorSecond+1:] {
					// rename element
					remainingSSTable := lvl.RenameFile(fileCounter, to, file)
					fileCounter++

					// append renamed to temp
					tempSSTables = append(tempSSTables, remainingSSTable)

				}
			}

			// renaming temo files
			lvl.RenameLevel(tempSSTables)

			lvl.GenerateLevels()
			// calls between levels
			if len(tempSSTables) > CAPACITY {
				// lvl.BetweenLevels(to, to+1)
				fmt.Println("-- NAREDNA KOMPAKCIJA --")

			}
			return

		}
		// level second got emptied first -> rename the rest of level one
		if counterFirst != 0 && counterSecond == 0 {
			// terminal condition 2

			// empty the remaining first file
			firstHeaderRecord, lastHeaderRecord = lvl.EmptyFile(first, SSTable, currentCapacity, firstHeaderRecord, lastHeaderRecord, files, writers,
				bf, merkle, fileCounter, counter, offsetData, offsetIndex, tempSSTables)

			fmt.Println("OVAJ KOJI SE PREPISUJE -> ", first.Name())

			// // closing the last file
			// fileName := first.Name()
			// first.Close()
			// err := os.Remove(fileName)
			// if err != nil {
			// 	fmt.Println("Brisanje ispraznjenog fajla")
			// }

			// closing the last SSTable
			SSTable.CopyExistingToSummary(firstHeaderRecord, lastHeaderRecord, files, writers)
			SSTable.EncodeHelpers(bf, merkle)
			SSTable.CloseFiles(files)

			// renaming reamining files to temp files

			if iteratorFirst+1 <= len(firstLevel)-1 {
				for _, file := range firstLevel[iteratorFirst+1:] {
					// rename element
					remainingSSTable := lvl.RenameFile(fileCounter, to, file)
					fileCounter++

					// append renamed to temp
					tempSSTables = append(tempSSTables, remainingSSTable)

				}
			}

			// renaming temo files
			lvl.RenameLevel(tempSSTables)

			lvl.GenerateLevels()
			// calls between levels
			if len(tempSSTables) > CAPACITY {
				// lvl.BetweenLevels(to, to+1)
				fmt.Println("-- NAREDNA KOMPAKCIJA --")

			}
			return

		}

		if beginning {
			lvl.MoveBeginning(&beginning, &fileCounter, to, first, second, &tempSSTables,
				&counterFirst, &counterSecond, &iteratorFirst, &iteratorSecond, &firstLevel, &secondLevel, SSTableFirst, SSTableSecond)
			if beginning {
				continue
			}
		}

		// sequential processing of both files
		minimumFile, minimumRecord := lvl.GetMinimumRecord(recordFirst, recordSecond, first, second, &counterFirst, &counterSecond, &iteratorFirst, &iteratorSecond, &firstLevel, &secondLevel)

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
			SSTable = lvl.NewSSTable(fileCounter, to, true)
			currentCapacity = int(minimumRecord.GetSize())
			// initializing all necesarry files
			files, writers, counter, offsetData, offsetIndex, bf, merkle = lvl.InitSSTable(SSTable)

			tempSSTables = append(tempSSTables, SSTable)

			firstHeaderRecord = minimumRecord

			// add record
			offsetData, offsetIndex = SSTable.AddRecord(counter, offsetData, offsetIndex, minimumRecord, bf, merkle, writers)
			counter++

		} else {
			// add record
			if firstHeaderRecord == nil {
				firstHeaderRecord = minimumRecord
			}

			offsetData, offsetIndex = SSTable.AddRecord(counter, offsetData, offsetIndex, minimumRecord, bf, merkle, writers)
			counter++
		}

		fmt.Println("AFTER WRITING TO SSTABLE -> ", minimumRecord)

		// remembers the last record -> will be necessary in header later
		lastHeaderRecord = minimumRecord

		lvl.NextRecordBetweenLevels(minimumFile, first, recordFirst, recordSecond, second, &counterFirst, &counterSecond, &iteratorFirst, &iteratorSecond, &firstLevel, &secondLevel)

		fmt.Println("AFTER NEXT RECORD -> ", minimumRecord)

	}

}

func (lvl *Leveled) GetMinimumRecord(recordFirst, recordSecond *record.Record, first, second *os.File, counterFirst, counterSecond, iteratorFirst, iteratorSecond *int, firstLevel, secondLevel *[]string) (*os.File, *record.Record) {
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

			// TODO MOVE TO THJE NEXT RECORD IN LOWER FILE
			lvl.NextRecordBetweenLevels(second, first, recordFirst, recordSecond, second, counterFirst, counterSecond, iteratorFirst, iteratorSecond, firstLevel, secondLevel)
			return first, recordFirst

		} else {

			// TODO MOVE TO THJE NEXT RECORD IN LOWER FILE
			lvl.NextRecordBetweenLevels(first, first, recordFirst, recordSecond, second, counterFirst, counterSecond, iteratorFirst, iteratorSecond, firstLevel, secondLevel)
			return second, recordSecond
		}
	}
}

func (lvl *Leveled) NextRecordBetweenLevels(minimumFile *os.File, first *os.File, recordFirst *record.Record, recordSecond *record.Record,
	second *os.File, counterFirst *int, counterSecond *int, iteratorFirst *int, iteratorSecond *int, firstLevel *[]string, secondLevel *[]string) {

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
				recordFirst, _ = record.ReadRecord(first)
				// read header -> update map
			}
		} else {
			*counterSecond--
			*iteratorSecond++

			if *iteratorSecond > len(*secondLevel)-1 {
				second = nil
			} else {
				second = lvl.lsm.OpenData((*secondLevel)[*iteratorSecond])
				recordSecond, _ = record.ReadRecord(second)
				// read header -> update map
			}
		}

		minimumFile.Close()
		delete(lvl.fromTo, minimumFile)

		// remove all files (DATA, INDEX, SUMMARY, BF, TOC, MERKLE)
		lvl.RemoveFile(minimumFile)

	} else {
		if minimumFile == first {
			recordFirst = nextRecord
		} else {
			recordSecond = nextRecord
		}
	}
}

func (lvl *Leveled) MoveBeginning(beginning *bool, fileCounter *int, to int, first, second *os.File, tempSSTables *[]*sstable.SStable,
	counterFirst, counterSecond, iteratorFirst, iteratorSecond *int, firstLevel, secondLevel *[]string, SSTableFirst, SSTableSecond *sstable.SStable) {

	if lvl.fromTo[first][0] < lvl.fromTo[second][0] && lvl.fromTo[first][1] < lvl.fromTo[second][0] {
		// renaming file from first level
		*tempSSTables = append(*tempSSTables, lvl.RenameFile(*fileCounter, to, first.Name()))
		delete(lvl.fromTo, first)
		// open next file from first level
		*counterFirst--
		*fileCounter++
		*iteratorFirst++

		if *iteratorFirst > len(*firstLevel)-1 {
			// no more files from level 1
			return
		} else {

			first = lvl.lsm.OpenData((*firstLevel)[*iteratorFirst])

			SSTableFirst = lvl.NewSSTableFromFileName(first)
			// read header and update map
			lvl.fromTo[first] = lvl.lsm.ReadHeader(SSTableFirst)

		}

		// continue
	} else if lvl.fromTo[second][0] < lvl.fromTo[first][0] && lvl.fromTo[second][1] < lvl.fromTo[first][0] {
		*tempSSTables = append(*tempSSTables, lvl.RenameFile(*fileCounter, to, second.Name()))
		delete(lvl.fromTo, second)
		// open next file from second level
		*counterSecond--
		*fileCounter++
		*iteratorSecond++

		if *iteratorSecond > len(*secondLevel)-1 {
			// no more files from level 2
			return
		} else {

			second = lvl.lsm.OpenData((*secondLevel)[*iteratorSecond])

			SSTableSecond = lvl.NewSSTableFromFileName(second)
			// read header and update map
			lvl.fromTo[second] = lvl.lsm.ReadHeader(SSTableSecond)

		}
	} else {
		*beginning = false
	}
}
