package writepath

import (
	configreader "NAiSP/Structures/ConfigReader"
	record "NAiSP/Structures/Record"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

// wal_size: 10
// wal_config.wal_config.WalBufferCapacity: 3
// max_number_of_segments: 10

// WalSize             int           `yaml:"wal_size"`
// WalBufferCapacity   int           `yaml:"wal_config.WalBufferCapacity"`
// MaxNumberOfSegments int

// SEGMENT_CAPACITY           = 10
// MAXIMUM_NUMBER_OF_SEGMENTS = 5

type WAL struct {
	Buffer              []*record.Record
	RecordsInSegment    uint
	CurrentLog          string
	MaxNumberOfSegments uint
	Config              *configreader.ConfigReader
	WritePath           *WritePath
	LastRecord          *record.Record
}

func NewWal(config *configreader.ConfigReader, wp *WritePath, lastRecord *record.Record) *WAL {

	records := make([]*record.Record, 0, config.WalBufferCapacity)
	firstlog := "../NAiSP/Data/Wal/wal_001.log"

	return &WAL{
		Buffer:              records,
		RecordsInSegment:    0,
		CurrentLog:          firstlog,
		MaxNumberOfSegments: 1,
		Config:              config,
		WritePath:           wp,
		LastRecord:          lastRecord,
	}
}

// Function for adding new Record
func (wal *WAL) AddRecord(rec *record.Record) bool {

	// Create record and open file for adding new record
	// rec := record.NewRecordKeyValue(key, value, tombstone)
	file, err := os.OpenFile(wal.CurrentLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		fmt.Println("Neuspesno otvoren fajl. ")
		panic(err)
	}
	defer file.Close()

	// Check if we reached max number of records in segment
	if wal.RecordsInSegment == uint(wal.Config.WalSize) {

		// Create new log and close current file
		// fmt.Println("NAPUNIO SE CEO SEGMENT. ")
		wal.RecordsInSegment = 0
		wal.AddLog()
		file.Close()

		wal.MaxNumberOfSegments += 1
		// Delete segments if we reached max number of segments
		if wal.MaxNumberOfSegments == uint(wal.Config.MaxNumberOfSegments) {
			wal.DeleteSegments()
		}
		file1, err := os.OpenFile(wal.CurrentLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

		if err != nil {
			fmt.Println("Neuspesno otvoren fajl. ")
			panic(err)
		}
		defer file1.Close()
		file = file1

	}

	file.Write(rec.Data)
	wal.RecordsInSegment += 1

	return true
}

// Function for adding new Record using buffer
func (wal *WAL) AddRecordBuffered(rec *record.Record) bool {

	// Create record and add to buffer
	// rec := record.NewRecordKeyValue(key, value, tombstone)
	// tombstone := rec.GetTombStone()
	wal.Buffer = append(wal.Buffer, rec)

	// If buffer full write in memory
	if len(wal.Buffer) == cap(wal.Buffer) {

		// Open file for writing
		// fmt.Println("Buffer je pun, sledi upis sledecih elemenata: ")
		file, err := os.OpenFile(wal.CurrentLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

		if err != nil {
			fmt.Println("Neuspesno otvoren fajl. ")
			panic(err)
		}
		defer file.Close()

		for _, record := range wal.Buffer {

			// Check if we reached max number of records in segment
			if wal.RecordsInSegment == uint(wal.Config.WalSize) {

				// fmt.Println("NAPUNIO SE CEO SEGMENT. ")
				wal.RecordsInSegment = 0
				wal.AddLog()
				file.Close()

				wal.MaxNumberOfSegments += 1
				// Delete segments if we reached max number of segments
				if wal.MaxNumberOfSegments == uint(wal.Config.MaxNumberOfSegments) {
					wal.DeleteSegments()
				}
				file1, err := os.OpenFile(wal.CurrentLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

				if err != nil {
					fmt.Println("Neuspesno otvoren fajl. ")
					panic(err)
				}
				defer file1.Close()
				file = file1
			}

			file.Write(record.Data)
			wal.RecordsInSegment += 1

		}

		// Empty buffer
		wal.Buffer = make([]*record.Record, 0, wal.Config.WalBufferCapacity)
	}

	return true
}

// Helper function to add new Log
func (wal *WAL) AddLog() {

	// Unpacking current log
	stringWithoutSuff := strings.TrimSuffix(wal.CurrentLog, ".log")
	numberString := strings.TrimPrefix(stringWithoutSuff, "../NAiSP/Data/Wal/wal_")

	// to keep format of number part (00n) - len: 3
	initialLength := len(numberString)
	// removed 0 from beginning - from 00n to n
	newString := strings.TrimLeft(numberString, "0")

	// from string n to number n
	number, _ := strconv.Atoi(newString)
	number += 1

	newNumberString := strconv.Itoa(number)
	// adding 0 back in new string
	newNumberString = fmt.Sprintf("%0*d", initialLength, number)

	// New log for 1 greater than last
	newLog := "../NAiSP/Data/Wal/wal_" + newNumberString + ".log"
	wal.CurrentLog = newLog

}

// Helper function for getting number from path
func GetNumberFromPath(s string) (int, error) {

	stringWithoutSuff := strings.TrimSuffix(s, ".log")
	numberString := strings.TrimPrefix(stringWithoutSuff, "wal_")

	// removed 0 from beginning - from 00n to n
	newString := strings.TrimLeft(numberString, "0")
	// from string n to number n
	number, err := strconv.Atoi(newString)

	return number, err
}

// Function for deleting all not needed segments
func (wal *WAL) DeleteSegments() {

	dir := "../NAiSP/Data/Wal"

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Iterate through files in dir
	for _, file := range files {
		// fmt.Println(file.Name())
		number, _ := GetNumberFromPath(file.Name())

		// Leaving only two newest segments
		if number == (wal.Config.MaxNumberOfSegments - 1) {
			continue
		}
		if number == wal.Config.MaxNumberOfSegments {
			continue
		}

		// Remove old segments
		err := os.Remove(dir + "/" + file.Name())
		if err != nil {
			fmt.Println(err)
		}
	}
	wal.RenameSegments()

}

// Function for renaming path after deleting segments
func (wal *WAL) RenameSegments() {

	// Path
	dir := "../NAiSP/Data/Wal"

	// Reading directory
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Iterate through files in dir
	for _, file := range files {
		// fmt.Println(file.Name())
		number, _ := GetNumberFromPath(file.Name())

		// Renaming two segments to be the new two oldest
		if number == (wal.Config.MaxNumberOfSegments - 1) {
			newPath := "../NAiSP/Data/Wal/wal_001.log"
			// Renaming
			err := os.Rename(dir+"/"+file.Name(), newPath)
			if err != nil {
				fmt.Println(err)
			}
		}
		if number == wal.Config.MaxNumberOfSegments {
			newPath := "../NAiSP/Data/Wal/wal_002.log"
			// Renaming
			err := os.Rename(dir+"/"+file.Name(), newPath)
			if err != nil {
				fmt.Println(err)
			}

		}

	}
	wal.CurrentLog = "../NAiSP/Data/Wal/wal_002.log"
	wal.MaxNumberOfSegments = 2
}

func (wal *WAL) calculateReconstruction() int {

	return int(math.Ceil(float64(wal.Config.MemtableSize/wal.Config.WalSize)) + 1)
}

func SortFiles(files []string) []string {

	mapFiles := make(map[int]string)

	// Need to store ints
	var keys []int
	// Need to store values
	var values []string

	for _, file := range files {

		number, _ := GetNumberFromPath(file)
		keys = append(keys, number)
		mapFiles[number] = file
	}

	sort.Ints(keys)
	for _, k := range keys {
		values = append(values, mapFiles[k])
	}

	return values
}

// NOT FINISHED YET, WAITING FOR MMAP MECHANISM
// Function for loading the newest log - only one that we need
func (wal *WAL) Reconstruction() bool {

	toReconstruct := wal.calculateReconstruction()

	// Path
	dir := "../NAiSP/Data/Wal"

	// Reading directory
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		fmt.Println(err)
	}

	fileNames := make([]string, 0)
	for _, file := range files {
		fileNames = append(fileNames, file.Name())
	}

	fileNames = SortFiles(fileNames)

	if len(fileNames) <= toReconstruct {
		// treba iscitati ceo files
		for _, fileName := range fileNames {
			file, err := os.Open(dir + "/" + fileName)
			if err != nil {
				fmt.Println("Greska kod citanja -> WAL")
			}
			for {
				record, _ := record.ReadRecord(file)
				if record == nil {
					break
				}
				if !record.CheckCRC() {
					continue
				}
				if wal.LastRecord == nil {
					wal.WritePath.Reconstruction(record)
					continue
				}
				if record.GetTimeStamp() > wal.LastRecord.GetTimeStamp() {
					wal.WritePath.Reconstruction(record)
					continue
				}

			}
			file.Close()
		}
	} else {
		for _, fileName := range fileNames[len(fileNames)-toReconstruct+1:] {

			file, err := os.Open(fileName)
			if err != nil {
				fmt.Println("Greska kod citanja -> WAL")
			}
			for {
				record, _ := record.ReadRecord(file)
				fmt.Println(record)
				if record == nil {
					break
				}
				if !record.CheckCRC() {
					continue
				}
				if wal.LastRecord == nil {
					wal.WritePath.Reconstruction(record)
					continue
				}
				if record.GetTimeStamp() > wal.LastRecord.GetTimeStamp() {
					wal.WritePath.Reconstruction(record)
					continue
				}

			}
			file.Close()
		}
	}

	for _, file := range files {
		// Remove old segments
		err := os.Remove(dir + "/" + file.Name())
		if err != nil {
			fmt.Println("Neuspesno brisanje wala", err)
		}
	}
	return true
}
