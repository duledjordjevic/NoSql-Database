package lsm

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	configreader "NAiSP/Structures/ConfigReader"
	merkle "NAiSP/Structures/Merkle"
	record "NAiSP/Structures/Record"
	sstable "NAiSP/Structures/Sstable"
	writepath "NAiSP/Structures/WritePath"
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

func ssRemoveFile(sufix string, dataPath string, config *configreader.ConfigReader) {

	os.Remove(dataPath + "/" + "Metadata" + sufix + ".txt")
	if config.DataFileStructure == "Single" {
		os.Remove(dataPath + "/" + "data" + sufix + ".bin")
		return
	}
	os.Remove(dataPath + "/" + "data" + sufix + ".bin")
	os.Remove(dataPath + "/" + "bloomfilter" + sufix + ".gob")
	os.Remove(dataPath + "/" + "index" + sufix + ".bin")
	os.Remove(dataPath + "/" + "summary" + sufix + ".bin")

}
func SizeTiered(config *configreader.ConfigReader) {

	lsmSizedTiered := LSM{Config: config}
	filePath := "./Data/Data" + config.DataFileStructure + "/" + config.Compaction
	dataPath := filePath + "/Data"
	tocPath := filePath + "/Toc/TOC"

	for j := 0; j < config.LSMLevelMax-1; j++ {
		// For data
		files, err := ioutil.ReadDir(dataPath)
		if err != nil {
			fmt.Println("Greska kod citanja direktorijuma - GenerateFileName: ", err)
			log.Fatal(err)
		}

		for i := 0; i < len(files); i++ {

			// Check if file starts with data
			startOfFile := strings.Split(files[i].Name(), "_")
			if startOfFile[0]+"_"+startOfFile[1] != "data_l"+strconv.FormatInt(int64(j), 10) {
				continue
			}

			// Store name of first file
			data1 := files[i].Name()

			// Check next file
			i++
			if i == len(files) {
				break
			}
			startOfNextFile := strings.Split(files[i].Name(), "_")
			if startOfNextFile[0]+"_"+startOfNextFile[1] != "data_l"+strconv.FormatInt(int64(j), 10) {
				continue
			}

			// Store name of second file
			data2 := files[i].Name()

			if startOfFile[1] == startOfNextFile[1] {
				// Compact
				openData1 := lsmSizedTiered.OpenData(dataPath + "/" + data1)
				openData2 := lsmSizedTiered.OpenData(dataPath + "/" + data2)
				var ssTable *sstable.SStable
				if config.DataFileStructure == "Multiple" {
					ssTable = sstable.NewSStableAutomatic(writepath.GenerateSufix(dataPath, writepath.GetLevel(data1)+1), config)
				} else {
					ssTable = &sstable.SStable{
						SStableFilePath: dataPath + "/data" + writepath.GenerateSufix(dataPath, writepath.GetLevel(data1)+1) + ".bin",
						TOCFilePath:     tocPath + writepath.GenerateSufix(dataPath, writepath.GetLevel(data1)+1) + ".txt",
						MetaDataPath:    dataPath + "/Metadata" + writepath.GenerateSufix(dataPath, writepath.GetLevel(data1)+1) + ".txt"}
				}
				compactSizeTired(openData1, openData2, ssTable, config.DataFileStructure)

				sufix1 := "_" + startOfFile[1] + "_" + strings.Split(startOfFile[2], ".")[0]
				sufix2 := "_" + startOfNextFile[1] + "_" + strings.Split(startOfNextFile[2], ".")[0]
				openData1.Close()
				openData2.Close()
				// Delete files from Data
				ssRemoveFile(sufix1, dataPath, config)
				ssRemoveFile(sufix2, dataPath, config)

				// Delete Toc files
				os.Remove(tocPath + sufix1 + ".txt")
				os.Remove(tocPath + sufix2 + ".txt")

			}
		}
	}

}

func finishAdd(counter int, offsetData uint64, offsetIndex uint64, bf *bloomfilter.BloomFilter, merkle *merkle.MerkleTree,
	writers []*bufio.Writer, ssTable *sstable.SStable, data *os.File) (*record.Record, uint64, uint64, int) {
	var finishRecord *record.Record
	for {
		rec, err := record.ReadRecord(data)
		if err == io.EOF {
			return finishRecord, offsetData, offsetIndex, counter
		}
		finishRecord = rec
		offsetData, offsetIndex, counter = addRecord(counter, offsetData, offsetIndex, rec, bf, merkle, writers, ssTable)
	}

}

func compactSizeTired(data1 *os.File, data2 *os.File, ssTable *sstable.SStable, structures string) bool {

	var files []*os.File
	if structures == "Multiple" {
		files = ssTable.CreateFiles()
	} else {
		files = ssTable.CreateExistingFiles()
	}

	writers := ssTable.CreateWriters(files)
	counter := 1
	offsetData := uint64(0)
	offsetIndex := uint64(0)
	bf := bloomfilter.NewBLoomFilter(100, 0.01)
	merkle := merkle.NewMerkleTreeFile(ssTable.MetaDataPath)

	var firstRecord *record.Record
	var finishRecord *record.Record

	// poslednje vrednosti rec1 i rec2
	var recCheck1 *record.Record
	var recCheck2 *record.Record

	// da li je drugi fajl doso do kraja
	errorCheck := false

	// samo za prvu iteraciju da sacuvamo vrednost rec1
	checkIteration := true

	// za proveru da li  je prvi elemanat sacuvan
	firstWriteRecord := false

	// da li je su predhodno rec1 = rec2
	equalsCheck := false
	var rec2 *record.Record

	for {
		fmt.Println("###########################")
		fmt.Println("Offset index: ", offsetIndex)
		fmt.Println("###########################")
		rec1, err1 := record.ReadRecord(data1)

		if err1 == io.EOF {
			// zavrsi upis samo sa drugom
			if !errorCheck {
				fmt.Println(" ovdeee 1")
				fmt.Println(recCheck1)
				fmt.Println(recCheck2)
				fmt.Println(rec2)
				if recCheck2.GetKey() == rec2.GetKey() && recCheck1.GetKey() != recCheck2.GetKey() {
					offsetData, offsetIndex, counter = addRecord(counter, offsetData, offsetIndex, recCheck2, bf, merkle, writers, ssTable)
				}
				var chrecord *record.Record
				chrecord, offsetData, offsetIndex, counter = finishAdd(counter, offsetData, offsetIndex, bf, merkle, writers, ssTable, data2)
				if chrecord != nil {
					finishRecord = chrecord
				}
			}
			break
		}
		// Ako je drugi fajl doso do kraja upisi slog i idi do kraja u prvo
		if errorCheck {
			fmt.Println(" ovdeee 2")
			offsetData, offsetIndex, counter = addRecord(counter, offsetData, offsetIndex, rec1, bf, merkle, writers, ssTable)
			var chrecord *record.Record
			chrecord, offsetData, offsetIndex, counter = finishAdd(counter, offsetData, offsetIndex, bf, merkle, writers, ssTable, data2)
			if chrecord != nil {
				finishRecord = chrecord
			}
			break
		}
		// Check for first iteration
		if checkIteration {
			recCheck1 = rec1
			checkIteration = false
		}
		for {
			fmt.Println("###########################")
			fmt.Println("Offset index: ", offsetIndex)
			fmt.Println("###########################")
			rec2 = rec1
			if equalsCheck {
				rec2tmp1, err2 := record.ReadRecord(data2)
				if err2 == io.EOF {
					fmt.Println("rekord 1: ", rec1.String())
					fmt.Println("rekord 2: ", rec2.String())
					offsetData, offsetIndex, counter = addRecord(counter, offsetData, offsetIndex, rec1, bf, merkle, writers, ssTable)
					recCheck2 = nil
					errorCheck = true
					break
				}
				rec2 = rec2tmp1
			} else {

				if recCheck1 != rec1 {
					rec2 = recCheck2

				} else {
					rec2tmp, err2 := record.ReadRecord(data2)
					if err2 == io.EOF {
						fmt.Println("rekord 1: ", rec1.String())
						fmt.Println("rekord 2: ", rec2.String())
						offsetData, offsetIndex, counter = addRecord(counter, offsetData, offsetIndex, rec1, bf, merkle, writers, ssTable)
						errorCheck = true
						recCheck2 = nil
						break
					}
					rec2 = rec2tmp
				}
			}
			// fmt.Println(rec2.GetKey())
			if rec1.GetKey() < rec2.GetKey() {
				offsetData, offsetIndex, counter = addRecord(counter, offsetData, offsetIndex, rec1, bf, merkle, writers, ssTable)
				finishRecord = rec1
				recCheck1 = rec1
				recCheck2 = rec2
				fmt.Println("rekord 1: ", rec1.String())
				fmt.Println("rekord 2: ", rec2.String())
				if !firstWriteRecord {
					firstRecord = rec1
					firstWriteRecord = true
				}
				equalsCheck = false
				break

			} else if rec1.GetKey() > rec2.GetKey() {
				offsetData, offsetIndex, counter = addRecord(counter, offsetData, offsetIndex, rec2, bf, merkle, writers, ssTable)
				finishRecord = rec2
				recCheck1 = rec1
				fmt.Println("rekord 1: ", rec1.String())
				fmt.Println("rekord 2: ", rec2.String())
				if !firstWriteRecord {
					firstRecord = rec2
					firstWriteRecord = true
				}
				equalsCheck = false
				continue

			} else {
				fmt.Println("Isti su: ")
				fmt.Println("rekord 1: ", rec1.String())
				fmt.Println("rekord 2: ", rec2.String())
				if rec1.GetTimeStamp() > rec2.GetTimeStamp() {
					offsetData, offsetIndex, counter = addRecord(counter, offsetData, offsetIndex, rec1, bf, merkle, writers, ssTable)
					finishRecord = rec1
					recCheck1 = rec1
					recCheck2 = rec2
					if !firstWriteRecord {
						firstRecord = rec1
						firstWriteRecord = true
					}
					equalsCheck = true
					break
				}

				offsetData, offsetIndex, counter = addRecord(counter, offsetData, offsetIndex, rec2, bf, merkle, writers, ssTable)
				finishRecord = rec2
				recCheck1 = rec1

				if !firstWriteRecord {
					firstRecord = rec2
					firstWriteRecord = true
				}
				equalsCheck = true
				fmt.Println("recChec2: ", recCheck2)
				fmt.Println("recChec1: ", recCheck1)
				break

			}
		}
	}

	if structures == "Multiple" {
		fmt.Println("First and Last: ")
		fmt.Println("First -> ", firstRecord)
		fmt.Println("Last -> ", finishRecord)
		fmt.Println("Counter -> ", counter)
		fmt.Println("Offset data: ", offsetData)
		fmt.Println("Offset index: ", offsetIndex)
		ssTable.CopyExistingToSummary(firstRecord, finishRecord, files, writers)
		ssTable.EncodeHelpers(bf, merkle)
		ssTable.CloseFiles(files)
	} else {
		fmt.Println("First and Last: ")
		fmt.Println("First -> ", firstRecord)
		fmt.Println("Last -> ", finishRecord)
		fmt.Println("Counter -> ", counter)
		fmt.Println("Offset data: ", offsetData)
		fmt.Println("Offset index: ", offsetIndex)
		ssTable.CopyExistingToSummary(firstRecord, finishRecord, files, writers)
		ssTable.EncodeHelpersOneFile(bf, merkle)

		bloomSize, summarySize := ssTable.CalculateFileSizes(files)
		sizes := []uint64{bloomSize, summarySize, offsetIndex}

		ssTable.CopyAllandWriteHeader(sizes, files, writers)

		ssTable.CloseFiles(files)

	}

	return true
}
func addRecord(counter int, offsetData uint64, offsetIndex uint64, record *record.Record,
	bf *bloomfilter.BloomFilter, merkle *merkle.MerkleTree, writers []*bufio.Writer, ssTable *sstable.SStable) (uint64, uint64, int) {

	offsetData, offsetIndex = ssTable.AddRecord(counter, offsetData, offsetIndex, record, bf, merkle, writers)
	counter++

	return offsetData, offsetIndex, counter
}
