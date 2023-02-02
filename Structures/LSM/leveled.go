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
				fmt.Println(filename)
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
	newLevel1 := make([]*sstable.SStable, 0)

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
	} else {
		records[first] = nil
	}

	// file containing minimal record
	minimumFile := first

	SSTable := NewSSTable(0, lsm)
	files, writers, counter, offsetData, offsetIndex, bf, merkle := InitSSTable(SSTable)
	newLevel1 = append(newLevel1, SSTable)

	// necessary for generating filename
	fileCounter := 0

	// necessary for checking if file is full
	currentCapacity := 0

	// it will be necessary to have this info for header later
	firstRecord := records[minimumFile]
	lastRecord := records[minimumFile]

	for {
		// lastRecord = records[minimumFile]

		if (counter0 + counter1) == 0 {
			// calls compaction for other levels if necessary
			if len(newLevel1) > CAPACITY {
				BetweenLevels(directory, levels, lsm)
			}
			// end of compaction
			return
		}
		if counter0 == 0 && counter1 != 0 {
			// moving remaining files to new level 1

			// TODO
			if len(newLevel1) > CAPACITY {
				BetweenLevels(directory, levels, lsm)
			}
			return
		}

		// searching for the smallest key and the most fresh
		for file, record := range records {
			if file != nil {
				if record.GetKey() < records[minimumFile].GetKey() {
					minimumFile = file
				} else if record.GetKey() == records[minimumFile].GetKey() {
					if record.GetTimeStamp() > records[minimumFile].GetTimeStamp() {
						minimumFile = file
					}
				}
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
			SSTable = NewSSTable(fileCounter, lsm)
			currentCapacity = int(records[minimumFile].GetSize())
			// initializing all necesarry files
			files, writers, counter, offsetData, offsetIndex, bf, merkle = InitSSTable(SSTable)

			firstRecord = records[minimumFile]

			// add record
			offsetData, offsetIndex = SSTable.AddRecord(counter, offsetData, offsetIndex, records[minimumFile], bf, merkle, writers)
			counter++
			// TODO
		} else {
			// add record
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

func AddRecord(SSTable *sstable.SStable) {

	// for _, record := range *records {
	// 	offsetData, offsetIndex = table.AddRecord(counter, offsetData, offsetIndex, record, bf, merkle, writers)
	// 	counter++
	// }
	// fmt.Println("First and Last: ")
	// first := (*records)[0]
	// fmt.Println("First -> ", first.String())
	// last := (*records)[len((*records))-1]
	// fmt.Println("Last -> ", last.String())
	// table.CopyExistingToSummary(first, last, files, writers)
	// table.EncodeHelpers(bf, merkle)
	// table.CloseFiles(files)

	// PrintSummary(table.SummaryPath)
	// PrintIndexTable(table.IndexTablePath)

}

// updates records map with new record -> if EOF deletes file and opens next if first
func NextRecord(directory string, minimumFile *os.File, first *os.File, counter0 *int, counter1 *int,
	iterator *int, records *map[*os.File]*record.Record, levels *map[int][]string, lsm *LSM) {
	(*records)[minimumFile], _ = record.ReadRecord(minimumFile)
	// EOF -> close the file and open another from level 1 if needed
	if (*records)[minimumFile] == nil {

		// open next file from level 1
		if minimumFile == first {
			*counter1--
			*iterator++
			first = lsm.OpenData((*levels)[1][*iterator])
			(*records)[first], _ = record.ReadRecord(first)
			minimumFile = first

		} else {
			*counter0--
		}

		minimumFile.Close()
		delete(*records, minimumFile)

		// file deletion

		// remove all files (DATA, INDEX, SUMMARY, BF, TOC, MERKLE)
		RemoveFile(minimumFile, lsm)
	}
}

func RemoveFile(file *os.File, lsm *LSM) {

	// TREBA DODATI PROVERU MRTVU DA LI OVI FAJLOVI POSTOJE
	// -> TO POGLEDAJ PA DODAJ DOLE U FUNKCIJE REMOVE-A
	// TODO
	data := "./Data/Data" + lsm.Config.DataFileStructure + "/" + lsm.Config.Compaction + "/Data/" + file.Name()

	index := strings.ReplaceAll(data, "data", "index")
	summary := strings.ReplaceAll(data, "data", "summary")
	bloomfilter := strings.ReplaceAll(data, "data", "bloomfilter")
	bloomfilter = strings.ReplaceAll(bloomfilter, ".bin", ".gob")
	merkle := strings.ReplaceAll(data, "data", "Metadata")
	merkle = strings.ReplaceAll(merkle, ".bin", ".txt")

	TOC := strings.ReplaceAll(data, "data", "TOC")
	TOC = strings.ReplaceAll(TOC, lsm.Config.DataFileStructure+"/"+"Data", lsm.Config.DataFileStructure+"/"+"Toc")
	TOC = strings.ReplaceAll(TOC, ".bin", ".txt")

	err := os.Remove(data)
	if err != nil {
		fmt.Println("Greska kod brisanja DATA datoteke", err)
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

func NewSSTable(counter int, lsm *LSM) *sstable.SStable {
	// directory := DIRECTORY + lsm.Config.DataFileStructure + "/" + lsm.Config.Compaction + "/Data"

	// creating new SSTable -> filename format: data_TEMP_counter.bin
	SSTable := sstable.NewSStableAutomatic(TEMPORARY_NAME+"_"+strconv.FormatInt(int64(counter), 10), lsm.Config)
	return SSTable
}

func BetweenLevels(directory string, levels *map[int][]string, lsm *LSM) {
	fmt.Println("---- Kurcina ----")
}
