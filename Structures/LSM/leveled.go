package lsm

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	configreader "NAiSP/Structures/ConfigReader"
	merkle "NAiSP/Structures/Merkle"
	readpath "NAiSP/Structures/ReadPath"
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

const (
	DIRECTORY        = "./Data/DataMultiple/Leveled/Data"
	CAPACITY         = 5
	TEMPORARY_NAME   = "_TEMP_"
	SSTABLE_CAPACITY = 200
	PREFIX           = "./Data/Data"
	SUFIX            = "/Data"
	PERCENT          = 0.7
)

type Leveled struct {
	lsm       *LSM
	directory string
	levels    map[int][]string
	config    *configreader.ConfigReader
	records   map[*os.File]*record.Record
	fromTo    map[*os.File][]string
	first     *os.File
	second    *os.File

	BROJACRECORDA         int
	BROJACPRENETIHFAJLOVA int
}

func NewLeveled(config *configreader.ConfigReader, lsm *LSM) *Leveled {
	directory := PREFIX + config.DataFileStructure + "/" + config.Compaction + SUFIX
	return &Leveled{lsm: lsm, directory: directory, config: config, levels: make(map[int][]string)}
}

func (lvl *Leveled) GenerateLevels() {
	lvl.levels = make(map[int][]string)
	files, err := ioutil.ReadDir(lvl.directory)
	if err != nil {
		fmt.Println("Greska kod citanja direktorijuma: ", err)
		log.Fatal(err)
	}

	if strings.HasSuffix(lvl.directory, "Data") {
		lvl.directory = lvl.directory + "/"
	}

	var currentLevel int
	for _, file := range files {
		if strings.Contains(file.Name(), "data") && !strings.Contains(file.Name(), "Meta") {
			currentLevel = writepath.GetLevel(file.Name())
			_, contains := lvl.levels[currentLevel]
			filename := lvl.directory + file.Name()
			if contains {
				lvl.levels[currentLevel] = append(lvl.levels[currentLevel], filename)
				continue
			}
			lvl.levels[currentLevel] = []string{filename}
		}
	}

	for lev := range lvl.levels {
		lvl.levels[lev] = readpath.SortFiles(lvl.levels[lev])
	}

}

func (lvl *Leveled) Compaction() {

	lvl.GenerateLevels()

	_, contains := lvl.levels[0]
	if contains {
		lvl.ZeroToFirst()
	}
}

func (lvl *Leveled) ZeroToFirst() {
	zero := make([]*os.File, 0)
	var first *os.File
	counter0 := len(lvl.levels[0])
	counter1 := len(lvl.levels[1])

	lvl.BROJACRECORDA = 0

	iterator := 0

	// every newly generated sstable will be added to this list
	tempSSTables := make([]*sstable.SStable, 0)

	// oppening every file from level 0
	for _, filename := range lvl.levels[0] {
		zero = append(zero, lvl.lsm.OpenData(filename))
	}

	// openning the first file from level 1
	if len(lvl.levels[1]) != 0 {
		first = lvl.lsm.OpenData(lvl.levels[1][iterator])
	} else {
		first = nil
	}

	// map of file-record pairs -> helps with searching for the smallest key
	lvl.records = make(map[*os.File]*record.Record)

	// reading first record from every file
	for _, file := range zero {
		lvl.records[file], _ = record.ReadRecord(file)
	}

	// if first level is empty
	if first != nil {
		lvl.records[first], _ = record.ReadRecord(first)
	}
	// file containing minimal record
	var minimumFile *os.File
	for key := range lvl.records {
		minimumFile = key
		break
	}

	SSTable := lvl.NewSSTable(0, 1, true)
	files, writers, counter, offsetData, offsetIndex, bf, merkle := lvl.InitSSTable(SSTable)
	tempSSTables = append(tempSSTables, SSTable)

	// necessary for generating filename
	fileCounter := 0

	// necessary for checking if file is full
	currentCapacity := 0

	// it will be necessary to have this info for header later
	var firstRecord *record.Record
	var lastRecord *record.Record

	i := 0

	for {
		// fmt.Println("Iteracija -> ", i)
		i++
		if counter0 == 0 && counter1 == 0 {
			// terminal condition 1

			// var remaining *os.File
			// for key := range lvl.records {
			// 	remaining = key
			// 	break
			// }

			fmt.Println("Terminal condition 1")

			for key := range lvl.records {

				fmt.Println("Preostale datoteke: ", key.Name())
			}

			// fmt.Println("Poslednja datoteka sa 0-tog nivoa -> ", remaining.Name())

			// firstRecord, lastRecord = lvl.EmptyFile(remaining, SSTable, currentCapacity, firstRecord, lastRecord, files, writers,
			// 	bf, merkle, fileCounter, counter, offsetData, offsetIndex, tempSSTables)

			SSTable.CopyExistingToSummary(firstRecord, lastRecord, files, writers)

			SSTable.EncodeHelpersWithoutTOC(bf, merkle)
			SSTable.CloseFiles(files)

			// fmt.Println("Broj tempova 1 -> ", len(tempSSTables))
			lvl.RenameLevel(tempSSTables)

			lvl.GenerateLevels()

			if len(tempSSTables) > CAPACITY {
				lvl.BetweenLevels(1, 2)
			}
			// end of compaction
			return
		} else if counter0 == 0 && counter1 != 0 {

			// terminal condition 2

			fmt.Println("Terminal condition 2 ")

			fmt.Println("Datoteka koja se prazni -> ", first.Name())
			fmt.Println("Record od kog se prazni -> ", lvl.records[first])

			fmt.Println("SSTABELA PRE -> ", SSTable.DataTablePath)

			// empty the remaining first file
			//firstRecord, lastRecord, LASTRECORD, SSTable :=
			SSTable, tempSSTables = lvl.EmptyFile(first, SSTable, currentCapacity, firstRecord, lastRecord, files, writers,
				bf, merkle, &fileCounter, counter, offsetData, offsetIndex, &tempSSTables, 1, true, nil)

			// fmt.Println("First record nakon praznjenja first-a -> ", LASTRECORD)

			fmt.Println("SSTABELA POSLE -> ", SSTable.DataTablePath)
			// closing the last SSTable

			// renaming reamining files to temp files

			if iterator+1 <= len(lvl.levels[1])-1 {
				for _, firstFile := range lvl.levels[1][iterator+1:] {
					// rename element

					fmt.Println("Preostale datoteke -> ", firstFile)

					remainingSSTable := lvl.RenameFile(fileCounter, 1, firstFile)
					fileCounter++
					lvl.BROJACPRENETIHFAJLOVA++

					// append renamed to temp
					tempSSTables = append(tempSSTables, remainingSSTable)

				}
			}

			// renaming temo files
			lvl.RenameLevel(tempSSTables)

			lvl.GenerateLevels()
			// calls between levels
			if len(tempSSTables) > CAPACITY {
				lvl.BetweenLevels(1, 2)
			}

			return

		}

		var toDelete []*os.File

		if lvl.records[minimumFile] == nil {
			toDelete = append(toDelete, minimumFile)
			lvl.DeleteFiles(toDelete, nil)
		}

		toDelete = []*os.File{}

		for file, rec := range lvl.records {

			// fmt.Println(file.Name(), rec)

			// if file != nil {
			if rec.GetKey() < lvl.records[minimumFile].GetKey() {
				minimumFile = file
			} else if rec.GetKey() == lvl.records[minimumFile].GetKey() {
				if rec.GetTimeStamp() > lvl.records[minimumFile].GetTimeStamp() {
					// read next record for the smaller
					// read next record in minimum file

					nextRecord, _ := record.ReadRecord(minimumFile)
					if nextRecord == nil {
						toDelete = append(toDelete, minimumFile)
					} else {
						lvl.records[minimumFile] = nextRecord
					}

					minimumFile = file

				} else if rec.GetTimeStamp() < lvl.records[minimumFile].GetTimeStamp() {

					nextRecord, _ := record.ReadRecord(file)
					if nextRecord == nil {
						toDelete = append(toDelete, file)

					} else {
						lvl.records[file] = nextRecord
					}

					// read next record in current file
				}
			}

			// }
		}
		// remove minimum file from toDelete

		// fmt.Println("--------------- NAKON ODABIRA MINIMUMA ---------------")
		// for file, rec := range lvl.records {
		// 	fmt.Println(file.Name(), rec)
		// }

		// fmt.Println("Minimum file -> ", minimumFile.Name())
		// fmt.Println("RECORD ----------------> ", lvl.records[minimumFile])

		// delete read files
		lvl.DeleteFiles(toDelete, minimumFile)

		// fmt.Println("BEFORE APPENDING -> ", lvl.records[minimumFile])

		// add minimum record to new sstable

		// writing to new record

		currentCapacity += int(lvl.records[minimumFile].GetSize())

		// if current sstable reached capacity -> make a new one
		if currentCapacity > SSTABLE_CAPACITY {

			// completing formation of SSTable
			SSTable.CopyExistingToSummary(firstRecord, lastRecord, files, writers)
			SSTable.EncodeHelpersWithoutTOC(bf, merkle)
			SSTable.CloseFiles(files)
			// and closing it

			// Initialiazing new SSTABLE -> adding last record to new SSTABLE
			fileCounter++
			SSTable = lvl.NewSSTable(fileCounter, 1, true)
			currentCapacity = int(lvl.records[minimumFile].GetSize())
			// initializing all necesarry files
			files, writers, counter, offsetData, offsetIndex, bf, merkle = lvl.InitSSTable(SSTable)

			tempSSTables = append(tempSSTables, SSTable)

			firstRecord = lvl.records[minimumFile]

			// add record
			offsetData, offsetIndex = SSTable.AddRecord(counter, offsetData, offsetIndex, lvl.records[minimumFile], bf, merkle, writers)

			lvl.BROJACRECORDA++

			counter++
			// TODO
		} else {
			// add record
			if firstRecord == nil {
				firstRecord = lvl.records[minimumFile]
			}

			offsetData, offsetIndex = SSTable.AddRecord(counter, offsetData, offsetIndex, lvl.records[minimumFile], bf, merkle, writers)
			counter++

			lvl.BROJACRECORDA++
			// TODO
		}

		// remembers the last record -> will be necessary in header later
		lastRecord = lvl.records[minimumFile]

		// fmt.Println("Iterator pre next-a -> ", iterator)
		// if first != nil {
		// 	fmt.Println("First pre -> ", first.Name())
		// }

		// read next record from minimum
		minimumFile, first = lvl.NextRecord(minimumFile, first, &counter0, &counter1, &iterator)

		// fmt.Println("Iterator posle next-a -> ", iterator)
		// if first != nil {
		// 	fmt.Println("First nakon -> ", first.Name())
		// }

	}

}

func (lvl *Leveled) NextRecord(minimumFile *os.File, first *os.File, counter0 *int, counter1 *int,
	iterator *int) (*os.File, *os.File) {

	newRecord, _ := record.ReadRecord(minimumFile)

	// for _, fname := range lvl.levels[1] {
	// 	fmt.Println("Datoteke lvl 1 -> ", fname)
	// }

	// EOF -> close the file and open another from level 1 if needed
	if newRecord == nil {

		// open next file from level 1
		if minimumFile == first {
			*counter1--
			*iterator++

			fmt.Println("Iterator -> ", *iterator)

			if *iterator > len(lvl.levels[1])-1 {
				first = nil
			} else {

				first = lvl.lsm.OpenData(lvl.levels[1][*iterator])
				lvl.records[first], _ = record.ReadRecord(first)
			}

		} else {
			*counter0--
		}

		minimumFile.Close()
		delete(lvl.records, minimumFile)

		// remove all files (DATA, INDEX, SUMMARY, BF, TOC, MERKLE)
		lvl.RemoveFile(minimumFile)

		// update minimum file
		for key := range lvl.records {
			minimumFile = key
			break
		}

		// fmt.Println("IF BRANCH -> ", lvl.records[minimumFile])

	} else {
		lvl.records[minimumFile] = newRecord
		// fmt.Println("ELSE BRANCH -> ", lvl.records[minimumFile])
	}

	return minimumFile, first

}

func (lvl *Leveled) EmptyFile(file *os.File, SSTable *sstable.SStable, currentCapacity int,
	firstHeaderRecord *record.Record, lastHeaderRecord *record.Record, files []*os.File,
	writers []*bufio.Writer, bf *bloomfilter.BloomFilter, merkle *merkle.MerkleTree, fileCounter *int, counter int,
	offsetData uint64, offsetIndex uint64, tempSSTables *[]*sstable.SStable, level int, isZeroToFirst bool, minimalRecord *record.Record) (*sstable.SStable, []*sstable.SStable) {

	addedLast := false
	var nextRecord *record.Record

	for {
		// terminal condition
		if addedLast {
			nextRecord, _ = record.ReadRecord(file)
		} else {
			if isZeroToFirst {
				nextRecord = lvl.records[file]
			} else {
				if minimalRecord == nil {
					minimalRecord, _ = record.ReadRecord(file)
				}
				nextRecord = minimalRecord
			}

			addedLast = true
		}

		if nextRecord == nil {

			file.Close()

			delete(lvl.records, file)

			fmt.Println("SSTABELA TOKOM -> ", SSTable.DataTablePath)
			fmt.Println("File koji se brise -> ", file.Name())

			lvl.RemoveFile(file)

			fmt.Println("First record -> ", firstHeaderRecord)
			fmt.Println("Last record -> ", lastHeaderRecord)

			SSTable.CopyExistingToSummary(firstHeaderRecord, lastHeaderRecord, files, writers)
			SSTable.EncodeHelpersWithoutTOC(bf, merkle)
			SSTable.CloseFiles(files)

			return SSTable, *tempSSTables

		}

		if firstHeaderRecord == nil {
			firstHeaderRecord = nextRecord
		}

		currentCapacity += int(nextRecord.GetSize())
		// if current sstable reached capacity -> make a new one
		if currentCapacity > SSTABLE_CAPACITY {

			// completing formation of SSTable
			SSTable.CopyExistingToSummary(firstHeaderRecord, lastHeaderRecord, files, writers)
			SSTable.EncodeHelpersWithoutTOC(bf, merkle)
			SSTable.CloseFiles(files)
			// and closing it

			// Initialiazing new SSTABLE -> adding last record to new SSTABLE
			(*fileCounter)++
			SSTable = lvl.NewSSTable(*fileCounter, level, true)
			currentCapacity = int(nextRecord.GetSize())
			// initializing all necesarry files
			files, writers, counter, offsetData, offsetIndex, bf, merkle = lvl.InitSSTable(SSTable)

			*tempSSTables = append(*tempSSTables, SSTable)

			firstHeaderRecord = nextRecord

			// add record
			offsetData, offsetIndex = SSTable.AddRecord(counter, offsetData, offsetIndex, nextRecord, bf, merkle, writers)
			counter++

			lvl.BROJACRECORDA++
			// TODO
		} else {
			// add record

			offsetData, offsetIndex = SSTable.AddRecord(counter, offsetData, offsetIndex, nextRecord, bf, merkle, writers)
			counter++

			lvl.BROJACRECORDA++
			// TODO
		}

		// remembers the last record -> will be necessary in header later
		lastHeaderRecord = nextRecord

	}
}

func (lvl *Leveled) NewSSTable(index int, level int, isTemp bool) *sstable.SStable {
	// directory := DIRECTORY + lsm.Config.DataFileStructure + "/" + lsm.Config.Compaction + "/Data"
	var infix string
	if isTemp {
		infix = TEMPORARY_NAME
	} else {
		infix = ""
	}
	// creating new SSTable -> filename format: data_TEMP_counter.bin
	SSTable := sstable.NewSStableAutomatic(infix+"l"+strconv.FormatInt(int64(level), 10)+
		"_"+strconv.FormatInt(int64(index), 10), lvl.lsm.Config)
	return SSTable
}

func (lvl *Leveled) NewSSTableFromFileName(file *os.File) *sstable.SStable {

	// fmt.Println("FILENAME SSTABLE FROM FILE NAME -> ", file.Name())
	names := lvl.CreateNames(file.Name())
	return &sstable.SStable{DataTablePath: file.Name(), IndexTablePath: names[0], SummaryPath: names[1], BloomFilterPath: names[2], MetaDataPath: names[3], TOCFilePath: names[4], SStableFilePath: file.Name()}
}

func (lvl *Leveled) InitSSTable(SSTable *sstable.SStable) ([]*os.File, []*bufio.Writer, int, uint64, uint64, *bloomfilter.BloomFilter, *merkle.MerkleTree) {
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

func (lvl *Leveled) RenameFile(index int, level int, filename string) *sstable.SStable {
	index++
	file := lvl.lsm.OpenData(filename)

	oldNames := lvl.CreateNames(file.Name())
	oldNames = append(oldNames, file.Name())
	// 		_l..._...
	// _TEMP_l..._...
	newName := strings.ReplaceAll(file.Name(), "_l", TEMPORARY_NAME+"l")

	newName = lvl.ChangeIndex(newName, index)
	if level != 1 {
		newName = lvl.ChangeLevel(newName, level)
	}

	newNames := lvl.CreateNames(newName)
	newNames = append(newNames, newName)

	file.Close()

	for i := range newNames {
		err := os.Rename(oldNames[i], newNames[i])
		if err != nil {
			fmt.Println("Greska kod preimenovanja u TEMP:\n", err)
		}

	}

	return sstable.NewSStable(newName, newNames[0], newNames[1], newNames[2], newNames[3], newNames[4], "Linija 233 leveled")
}

func (lvl *Leveled) RenameLevel(SSTables []*sstable.SStable) {
	var filename string
	for _, SSTable := range SSTables {

		// fmt.Println("SSTABELA KOJA SE NE PREIMENUJE -> ", SSTable.DataTablePath)

		filename = SSTable.DataTablePath
		err := os.Rename(filename, strings.ReplaceAll(filename, TEMPORARY_NAME, "_"))
		if err != nil {
			fmt.Println("Greska kod preimenovanja DATA:\n", err)
		}

		SSTable.DataTablePath = strings.ReplaceAll(filename, TEMPORARY_NAME, "_")

		filename = SSTable.IndexTablePath
		err = os.Rename(filename, strings.ReplaceAll(filename, TEMPORARY_NAME, "_"))
		if err != nil {
			fmt.Println("Greska kod preimenovanja INDEX:\n", err)
		}

		SSTable.IndexTablePath = strings.ReplaceAll(filename, TEMPORARY_NAME, "_")

		filename = SSTable.SummaryPath
		err = os.Rename(filename, strings.ReplaceAll(filename, TEMPORARY_NAME, "_"))
		if err != nil {
			fmt.Println("Greska kod preimenovanja SUMMARY:\n", err)
		}

		SSTable.SummaryPath = strings.ReplaceAll(filename, TEMPORARY_NAME, "_")

		filename = SSTable.BloomFilterPath
		err = os.Rename(filename, strings.ReplaceAll(filename, TEMPORARY_NAME, "_"))
		if err != nil {
			fmt.Println("Greska kod preimenovanja BF:\n", err)
		}

		SSTable.BloomFilterPath = strings.ReplaceAll(filename, TEMPORARY_NAME, "_")

		filename = SSTable.MetaDataPath
		err = os.Rename(filename, strings.ReplaceAll(filename, TEMPORARY_NAME, "_"))
		if err != nil {
			fmt.Println("Greska kod preimenovanja METADATA:\n", err)
		}

		SSTable.MetaDataPath = strings.ReplaceAll(filename, TEMPORARY_NAME, "_")

		filename = SSTable.TOCFilePath
		SSTable.TOCFilePath = strings.ReplaceAll(filename, TEMPORARY_NAME, "_")

		// fmt.Println("Iznad form toc-a -> ", i)

		SSTable.FormTOC()

		// fmt.Println("Ispode form toc-a -> ", i)

		// filename = SSTable.TOCFilePath
		// err = os.Rename(filename, strings.ReplaceAll(filename, TEMPORARY_NAME, "_"))
		// if err != nil {
		// 	fmt.Println("Greska kod preimenovanja TOC:\n", err)
		// }

		// SSTable.FormTOC()

	}
}

func (lvl *Leveled) CreateNames(filename string) []string {

	// fmt.Println("FILENAME -> ", filename)

	if lvl.config.DataFileStructure == "Single" {
		index := filename
		summary := filename
		bloomfilter := filename

		merkle := strings.ReplaceAll(filename, "data", "Metadata")
		merkle = strings.ReplaceAll(merkle, ".bin", ".txt")

		TOC := strings.ReplaceAll(filename, "data", "TOC")
		TOC = strings.ReplaceAll(TOC, lvl.config.Compaction+"/Data", lvl.config.Compaction+"/Toc")
		TOC = strings.ReplaceAll(TOC, ".bin", ".txt")
		return []string{index, summary, bloomfilter, merkle, TOC}
	}

	index := strings.ReplaceAll(filename, "data", "index")
	summary := strings.ReplaceAll(filename, "data", "summary")
	bloomfilter := strings.ReplaceAll(filename, "data", "bloomfilter")
	bloomfilter = strings.ReplaceAll(bloomfilter, ".bin", ".gob")
	merkle := strings.ReplaceAll(filename, "data", "Metadata")
	merkle = strings.ReplaceAll(merkle, ".bin", ".txt")

	TOC := strings.ReplaceAll(filename, "data", "TOC")
	TOC = strings.ReplaceAll(TOC, lvl.config.Compaction+"/Data", lvl.config.Compaction+"/Toc")
	TOC = strings.ReplaceAll(TOC, ".bin", ".txt")

	return []string{index, summary, bloomfilter, merkle, TOC}
}

func (lvl *Leveled) ChangeIndex(filename string, newIndex int) string {

	fmt.Println("Novi index -> ", newIndex)

	current := strings.Split(filename, "_")[3]
	// trenutni nivo
	current = strings.Split(current, ".")[0]

	filename = strings.ReplaceAll(filename, current+".", strconv.FormatInt(int64(newIndex), 10)+".")

	fmt.Println("Novi naziv (index) -> ", filename)

	return filename
}

func (lvl *Leveled) ChangeLevel(filename string, newLevel int) string {
	current := strings.Split(filename, "_")[2]
	current = strings.Split(current, "l")[1]

	filename = strings.ReplaceAll(filename, "_l"+current+"_", "_l"+strconv.FormatInt(int64(newLevel), 10)+"_")
	return filename
}

func (lvl *Leveled) DeleteFiles(files []*os.File, minimumFile *os.File) {
	// fmt.Println("TO BE DELETED: ")
	for _, file := range files {
		if file == minimumFile {
			continue
		}
		// fmt.Println(file.Name())
		delete(lvl.records, file)
		file.Close()
		lvl.RemoveFile(file)
	}
}

func (lvl *Leveled) RemoveFile(file *os.File) {

	data := file.Name()
	filenames := lvl.CreateNames(file.Name())

	err := os.Remove(data)
	if err != nil {
		fmt.Println("Greska kod brisanja DATA datoteke", err)
		fmt.Println("Ime fajla -> ", file.Name())
		fmt.Println("DATA -> ", data)
		return
	}
	err = os.Remove(filenames[0])
	if err != nil {
		fmt.Println("Greska kod brisanja INDEX datoteke", err)
		fmt.Println("-- Ili je sve u jednom fajlu --")
		// return
	}
	err = os.Remove(filenames[1])
	if err != nil {
		fmt.Println("Greska kod brisanja SUMMARY datoteke", err)
		fmt.Println("-- Ili je sve u jednom fajlu --")
		// return
	}
	err = os.Remove(filenames[2])
	if err != nil {
		fmt.Println("Greska kod brisanja BF datoteke", err)
		fmt.Println("-- Ili je sve u jednom fajlu --")
		// return
	}
	err = os.Remove(filenames[3])
	if err != nil {
		fmt.Println("Greska kod brisanja MERKLE datoteke", err)
		return
	}
	err = os.Remove(filenames[4])
	if err != nil {
		fmt.Println("Greska kod brisanja TOC datoteke", err)
		return
	}
}

// func RemoveElementFromList(slice []*os.File, file *os.File) {
// 	for _, f := range slice {
// 		if f == file {

// 		}
// 	}
// }
