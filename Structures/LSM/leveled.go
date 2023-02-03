package lsm

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	merkle "NAiSP/Structures/Merkle"
	record "NAiSP/Structures/Record"
	sstable "NAiSP/Structures/Sstable"
	writepath "NAiSP/Structures/WritePath"
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

// "./Data/Data" + config.DataFileStructure + "/" + config.Compaction + "/Data/"

const (
	DIRECTORY        = "./Data/DataMultiple/Leveled/Data"
	CAPACITY         = 10
	TEMPORARY_NAME   = "_TEMP_"
	SSTABLE_CAPACITY = 1024
	PREFIX           = "./Data/Data"
	SUFIX            = "/Data"
)

func Leveled(lsm *LSM) {
	directory := PREFIX + lsm.Config.DataFileStructure + "/" + lsm.Config.Compaction + SUFIX

	files, err := ioutil.ReadDir(directory)
	if err != nil {
		fmt.Println("Greska kod citanja direktorijuma: ", err)
		log.Fatal(err)
	}

	levels := make(map[int][]string)
	var currentLevel int
	for _, file := range files {
		if strings.Contains(file.Name(), "data") && !strings.Contains(file.Name(), "Meta") {
			currentLevel = writepath.GetLevel(file.Name())
			_, contains := levels[currentLevel]
			filename := directory + "/" + file.Name()
			if contains {
				// fmt.Println(filename)
				levels[currentLevel] = append(levels[currentLevel], filename)
				continue
			}
			levels[currentLevel] = []string{filename}
		}
	}
	_, contains := levels[0]
	if contains {
		ZeroToFirst(directory+"/", &levels, lsm)
	}

}

func ZeroToFirst(directory string, levels *map[int][]string, lsm *LSM) {
	zero := make([]*os.File, 0)
	var first *os.File
	counter0 := len((*levels)[0])
	counter1 := len((*levels)[1])

	iterator := 0

	// every newly generated sstable will be added to this list
	tempSSTables := make([]*sstable.SStable, 0)

	// oppening every file from level 0
	for _, filename := range (*levels)[0] {
		zero = append(zero, lsm.OpenData(filename))
	}

	// openning the first file from level 1
	if len((*levels)[1]) != 0 {
		first = lsm.OpenData((*levels)[1][iterator])
	} else {
		first = nil
	}

	// map of file-record pairs -> helps with searching for the smallest key
	records := make(map[*os.File]*record.Record)

	// reading first record from every file
	for _, file := range zero {
		records[file], _ = record.ReadRecord(file)
	}

	// if first level is empty
	if first != nil {
		records[first], _ = record.ReadRecord(first)
	}
	// file containing minimal record
	var minimumFile *os.File
	for key := range records {
		minimumFile = key
		break
	}

	SSTable := NewSSTable(0, 1, lsm)
	files, writers, counter, offsetData, offsetIndex, bf, merkle := InitSSTable(SSTable)
	tempSSTables = append(tempSSTables, SSTable)

	// necessary for generating filename
	fileCounter := 0

	// necessary for checking if file is full
	currentCapacity := 0

	// it will be necessary to have this info for header later
	var firstRecord *record.Record
	var lastRecord *record.Record

	for {

		if (counter0 + counter1) == 0 {
			// calls compaction for other levels if necessary

			SSTable.CopyExistingToSummary(firstRecord, lastRecord, files, writers)
			SSTable.EncodeHelpers(bf, merkle)
			SSTable.CloseFiles(files)

			fmt.Println("Broj tempova 1 -> ", len(tempSSTables))
			RenameLevel(tempSSTables)

			if len(tempSSTables) > CAPACITY {
				BetweenLevels(directory, levels, lsm)
			}
			// end of compaction
			return
		}
		if counter0 == 0 && counter1 != 0 {

			SSTable.CopyExistingToSummary(firstRecord, lastRecord, files, writers)
			SSTable.EncodeHelpers(bf, merkle)
			SSTable.CloseFiles(files)

			fmt.Println("Broj tempova 2 -> ", len(tempSSTables))

			// moving remaining files to new level 1
			RenameRemaining(fileCounter, (*levels)[1][iterator+1:], lsm)
			// TODO

			RenameLevel(tempSSTables)
			if len(tempSSTables) > CAPACITY {
				BetweenLevels(directory, levels, lsm)
			}
			return
		}

		// searching for the smallest key and the most fresh

		if records[minimumFile] == nil {
			fmt.Println("Ne valja minimum")
		}

		for file, record := range records {
			if file != nil {
				if records[minimumFile] != nil {
					if record.GetKey() < records[minimumFile].GetKey() {
						minimumFile = file
					} else if record.GetKey() == records[minimumFile].GetKey() {
						if record.GetTimeStamp() > records[minimumFile].GetTimeStamp() {
							minimumFile = file
						}
					}
				} else {
					minimumFile = file
				}

				// fmt.Println(records[minimumFile], record)

			}
		}

		// writing to new record
		currentCapacity += int(records[minimumFile].GetSize())
		// if current sstable reached capacity -> make a new one
		if currentCapacity > SSTABLE_CAPACITY {

			// completing formation of SSTable
			SSTable.CopyExistingToSummary(firstRecord, lastRecord, files, writers)
			SSTable.EncodeHelpers(bf, merkle)
			SSTable.CloseFiles(files)
			// and closing it

			// Initialiazing new SSTABLE -> adding last record to new SSTABLE
			fileCounter++
			SSTable = NewSSTable(fileCounter, 1, lsm)
			currentCapacity = int(records[minimumFile].GetSize())
			// initializing all necesarry files
			files, writers, counter, offsetData, offsetIndex, bf, merkle = InitSSTable(SSTable)

			tempSSTables = append(tempSSTables, SSTable)

			firstRecord = records[minimumFile]

			// add record
			offsetData, offsetIndex = SSTable.AddRecord(counter, offsetData, offsetIndex, records[minimumFile], bf, merkle, writers)
			counter++
			// TODO
		} else {
			// add record
			if firstRecord == nil {
				firstRecord = records[minimumFile]
			}

			offsetData, offsetIndex = SSTable.AddRecord(counter, offsetData, offsetIndex, records[minimumFile], bf, merkle, writers)
			counter++
			// TODO
		}

		// remembers the last record -> will be necessary in header later
		lastRecord = records[minimumFile]
		// read next record
		NextRecord(directory, minimumFile, first, &counter0, &counter1, &iterator, &records, levels, lsm)

	}
}

func InitSSTable(SSTable *sstable.SStable) ([]*os.File, []*bufio.Writer, int, uint64, uint64, *bloomfilter.BloomFilter, *merkle.MerkleTree) {
	// if first record InitSSTable will open all the files necessary
	files := SSTable.CreateFiles()
	writers := SSTable.CreateWriters(files)
	counter := 1
	offsetData := uint64(0)
	offsetIndex := uint64(0)
	bf := bloomfilter.NewBLoomFilter(100, 0.01)
	merkle := merkle.NewMerkleTreeFile(SSTable.MetaDataPath)
	return files, writers, counter, offsetData, offsetIndex, bf, merkle
}

// updates records map with new record -> if EOF deletes file and opens next if first
func NextRecord(directory string, minimumFile *os.File, first *os.File, counter0 *int, counter1 *int,
	iterator *int, records *map[*os.File]*record.Record, levels *map[int][]string, lsm *LSM) {

	newRecord, _ := record.ReadRecord(minimumFile)

	// EOF -> close the file and open another from level 1 if needed
	if newRecord == nil {

		// open next file from level 1
		if minimumFile == first {
			*counter1--
			*iterator++

			if *iterator > len((*levels)[1])-1 {
				first = nil

			} else {
				first = lsm.OpenData((*levels)[1][*iterator])
				(*records)[first], _ = record.ReadRecord(first)
			}

		} else {
			*counter0--
		}

		minimumFile.Close()
		delete(*records, minimumFile)

		// remove all files (DATA, INDEX, SUMMARY, BF, TOC, MERKLE)
		RemoveFile(minimumFile, lsm)

		// update minimum file
		for key := range *records {
			minimumFile = key
			break
		}

		fmt.Println("NOVI PRINT -> ", (*records)[minimumFile])

	} else {
		(*records)[minimumFile] = newRecord
	}

}

func RenameLevel(SSTables []*sstable.SStable) {
	var filename string
	for _, SSTable := range SSTables {
		filename = SSTable.DataTablePath
		err := os.Rename(filename, strings.ReplaceAll(filename, TEMPORARY_NAME, "_"))
		if err != nil {
			fmt.Println("Greska kod preimenovanja DATA")
		}

		filename = SSTable.IndexTablePath
		err = os.Rename(filename, strings.ReplaceAll(filename, TEMPORARY_NAME, "_"))
		if err != nil {
			fmt.Println("Greska kod preimenovanja INDEX")
		}

		filename = SSTable.SummaryPath
		err = os.Rename(filename, strings.ReplaceAll(filename, TEMPORARY_NAME, "_"))
		if err != nil {
			fmt.Println("Greska kod preimenovanja SUMMARY")
		}

		filename = SSTable.BloomFilterPath
		err = os.Rename(filename, strings.ReplaceAll(filename, TEMPORARY_NAME, "_"))
		if err != nil {
			fmt.Println("Greska kod preimenovanja BF")
		}

		filename = SSTable.MetaDataPath
		err = os.Rename(filename, strings.ReplaceAll(filename, TEMPORARY_NAME, "_"))
		if err != nil {
			fmt.Println("Greska kod preimenovanja METADATA")
		}

		filename = SSTable.TOCFilePath
		err = os.Rename(filename, strings.ReplaceAll(filename, TEMPORARY_NAME, "_"))
		if err != nil {
			fmt.Println("Greska kod preimenovanja TOC")
		}

	}
}

func RenameRemaining(index int, remaining []string, lsm *LSM) {
	for _, filename := range remaining {
		file := lsm.OpenData(filename)

		oldNames := CreateNames(file.Name(), lsm)
		// 		_l..._...
		// _TEMP_l..._...
		newName := strings.ReplaceAll(file.Name(), "_l", TEMPORARY_NAME+"l")

		newName = ChangeIndex(newName, index)
		index++

		newNames := CreateNames(newName, lsm)

		fmt.Println("Novi data -> ", newName)
		fmt.Println("Novi index -> ", newNames[0])

		for i := range newNames {
			err := os.Rename(oldNames[i], newNames[i])
			if err != nil {
				fmt.Println("Greska kod preimenovanja u TEMP")
			}
		}

		file.Close()

	}

}

func ChangeIndex(filename string, newIndex int) string {

	fmt.Println("FileName u currentu -> ", filename)

	current := strings.Split(filename, "_")[3]

	fmt.Println("Current pre splita po . -> ", current)
	// trenutni nivo
	current = strings.Split(current, ".")[0]
	fmt.Println("Current index -> ", current)

	filename = strings.ReplaceAll(filename, current, strconv.FormatInt(int64(newIndex), 10))

	return filename
}

func CreateNames(filename string, lsm *LSM) []string {

	index := strings.ReplaceAll(filename, "data", "index")
	summary := strings.ReplaceAll(filename, "data", "summary")
	bloomfilter := strings.ReplaceAll(filename, "data", "bloomfilter")
	bloomfilter = strings.ReplaceAll(bloomfilter, ".bin", ".gob")
	merkle := strings.ReplaceAll(filename, "data", "Metadata")
	merkle = strings.ReplaceAll(merkle, ".bin", ".txt")

	TOC := strings.ReplaceAll(filename, "data", "TOC")
	TOC = strings.ReplaceAll(TOC, lsm.Config.Compaction+"/Data", lsm.Config.Compaction+"/Toc")
	TOC = strings.ReplaceAll(TOC, ".bin", ".txt")

	return []string{index, summary, bloomfilter, merkle, TOC}

}

func RemoveFile(file *os.File, lsm *LSM) {

	// TREBA DODATI PROVERU MRTVU DA LI OVI FAJLOVI POSTOJE
	// -> TO POGLEDAJ PA DODAJ DOLE U FUNKCIJE REMOVE-A
	// TODO
	// data := PREFIX + lsm.Config.DataFileStructure + "/" + lsm.Config.Compaction + SUFIX + "/" + file.Name()

	data := file.Name()

	index := strings.ReplaceAll(data, "data", "index")
	summary := strings.ReplaceAll(data, "data", "summary")
	bloomfilter := strings.ReplaceAll(data, "data", "bloomfilter")
	bloomfilter = strings.ReplaceAll(bloomfilter, ".bin", ".gob")
	merkle := strings.ReplaceAll(data, "data", "Metadata")
	merkle = strings.ReplaceAll(merkle, ".bin", ".txt")

	TOC := strings.ReplaceAll(data, "data", "TOC")
	TOC = strings.ReplaceAll(TOC, lsm.Config.Compaction+"/Data", lsm.Config.Compaction+"/Toc")
	TOC = strings.ReplaceAll(TOC, ".bin", ".txt")

	err := os.Remove(data)
	if err != nil {
		fmt.Println("Greska kod brisanja DATA datoteke", err)
		fmt.Println("Ime fajla -> ", file.Name())
		fmt.Println("DATA -> ", data)
		return
	}
	err = os.Remove(index)
	if err != nil {
		fmt.Println("Greska kod brisanja INDEX datoteke", err)
		fmt.Println("-- Ili je sve u jednom fajlu --")
		// return
	}
	err = os.Remove(summary)
	if err != nil {
		fmt.Println("Greska kod brisanja SUMMARY datoteke", err)
		fmt.Println("-- Ili je sve u jednom fajlu --")
		// return
	}
	err = os.Remove(bloomfilter)
	if err != nil {
		fmt.Println("Greska kod brisanja BF datoteke", err)
		fmt.Println("-- Ili je sve u jednom fajlu --")
		// return
	}
	err = os.Remove(merkle)
	if err != nil {
		fmt.Println("Greska kod brisanja MERKLE datoteke", err)
		return
	}
	err = os.Remove(TOC)
	if err != nil {
		fmt.Println("Greska kod brisanja TOC datoteke", err)
		return
	}
}

func NewSSTable(counter int, level int, lsm *LSM) *sstable.SStable {
	// directory := DIRECTORY + lsm.Config.DataFileStructure + "/" + lsm.Config.Compaction + "/Data"

	// creating new SSTable -> filename format: data_TEMP_counter.bin
	SSTable := sstable.NewSStableAutomatic(TEMPORARY_NAME+"l"+strconv.FormatInt(int64(level), 10)+
		"_"+strconv.FormatInt(int64(counter), 10), lsm.Config)
	return SSTable
}

func BetweenLevels(directory string, levels *map[int][]string, lsm *LSM) {
	fmt.Println("---- Kurcina ----")
}
