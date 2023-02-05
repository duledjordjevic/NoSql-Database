package sstable

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	config "NAiSP/Structures/ConfigReader"
	merkle "NAiSP/Structures/Merkle"
	record "NAiSP/Structures/Record"
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	EXISTING         = "existing.bin"
	EXISTING_INDEX   = "existingIndex.bin"
	EXISTING_DATA    = "existingData.bin"
	EXISTING_SUMMARY = "existingSum.bin"
	EXISTING_BLOOM   = "existingBloom.bin"
	HEADER           = 8 + 8 + 8
)

type SStable struct {
	DataTablePath   string
	IndexTablePath  string
	SummaryPath     string
	BloomFilterPath string
	MetaDataPath    string
	TOCFilePath     string
	SStableFilePath string
}

func NewSStable(dataTable string, indexTable string, summary string, bloom string, meta string, tocPath string, sstablePath string) *SStable {
	sstable := SStable{DataTablePath: dataTable, IndexTablePath: indexTable, SummaryPath: summary, BloomFilterPath: bloom, MetaDataPath: meta, TOCFilePath: tocPath, SStableFilePath: sstablePath}
	return &sstable
}

func NewSStableAutomatic(sufix string, config *config.ConfigReader) *SStable {
	prefix := "./Data/Data" + config.DataFileStructure + "/" + config.Compaction + "/Data/"
	TOCprefix := "./Data/Data" + config.DataFileStructure + "/" + config.Compaction + "/Toc/"
	sstable := SStable{DataTablePath: prefix + "data" + sufix + ".bin",
		IndexTablePath:  prefix + "index" + sufix + ".bin",
		SummaryPath:     prefix + "summary" + sufix + ".bin",
		BloomFilterPath: prefix + "bloomfilter" + sufix + ".gob",
		MetaDataPath:    prefix + "Metadata" + sufix + ".txt",
		TOCFilePath:     TOCprefix + "TOC" + sufix + ".txt"}
	return &sstable
}

func (table *SStable) FormSStable(records *[]*record.Record) {
	file, err := os.Create(table.DataTablePath)
	if err != nil {
		fmt.Println("Error")
		return
	}
	defer file.Close()

	fileIndex, err := os.Create(table.IndexTablePath)
	if err != nil {
		fmt.Println("Error")
		return
	}
	defer fileIndex.Close()

	fileSumHeader, err := os.Create(table.SummaryPath)
	if err != nil {
		fmt.Println("Error")
	}
	defer fileSumHeader.Close()

	fileSumExisting, err := os.Create("existing.bin")
	if err != nil {
		fmt.Println("Error")
	}
	defer fileSumExisting.Close()

	writer := bufio.NewWriter(file)
	writerIndex := bufio.NewWriter(fileIndex)
	writerSum := bufio.NewWriter(fileSumHeader)
	writerSumEx := bufio.NewWriter(fileSumExisting)

	i := 1
	var firstRecord record.Record
	var lastRecord record.Record
	var recordForSummary record.Record
	currentSize := uint64(0)
	currentOffIndex := uint64(0)

	bf := bloomfilter.NewBLoomFilter(100, 0.01)
	merkle := merkle.NewMerkleTreeFile(table.MetaDataPath)

	for _, record := range *records {
		bf.Hash(record.GetKey())
		merkle.AddLeaf(record.Data)
		if i == 1 {
			firstRecord = *record
			recordInsertSum := NewSummary(firstRecord.GetKey(), currentOffIndex)
			recordInsertSum.WriteSummary(writerSumEx)
		}
		if i == len(*records) {
			lastRecord = *record
		}
		WriteDataTable(record, writer)

		index := NewIndex(record.GetKey(), uint64(currentSize))
		index.WriteIndexTable(writerIndex)
		currentSize += record.GetSize()

		currentOffIndex += index.GetSize()
		if i%5 == 0 {
			recordForSummary = *record

			recordInsertSum := NewSummary(recordForSummary.GetKey(), currentOffIndex)
			recordInsertSum.WriteSummary(writerSumEx)
		}
		i++
	}
	bf.Encode(table.BloomFilterPath)
	merkle.GenerateMerkleTree()
	merkle.Encode()
	table.FormTOC()

	fileSumExisting.Seek(0, 0)
	sum := NewSummaryHeader(firstRecord.GetKey(), lastRecord.GetKey())
	sum.WriteSummaryHeader(writerSum)

	_, err = io.Copy(fileSumHeader, fileSumExisting)
	if err != nil {
		fmt.Println("Error")
		return
	}
	fileSumExisting.Close()
	err = os.Remove("existing.bin")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	// PrintSummary(table.SummaryPath)
	// PrintDataTable(table.DataTablePath)
	// PrintIndexTable(table.IndexTablePath)
}

func (table *SStable) CreateFiles() []*os.File {
	files := make([]*os.File, 0)
	file, err := os.Create(table.DataTablePath)
	if err != nil {
		fmt.Println("Error data file")
		panic(err)
	}
	files = append(files, file)

	file, err = os.Create(table.IndexTablePath)
	if err != nil {
		fmt.Println("Error index file")
		panic(err)
	}
	files = append(files, file)

	file, err = os.Create(table.SummaryPath)
	if err != nil {
		fmt.Println("Error summary file")
		panic(err)
	}
	files = append(files, file)

	file, err = os.Create(EXISTING)
	if err != nil {
		fmt.Println("Error existing.bin file")
		panic(err)
	}
	files = append(files, file)

	return files
}

func (table *SStable) CreateWriters(files []*os.File) []*bufio.Writer {
	writers := make([]*bufio.Writer, 0)
	for _, file := range files {
		writers = append(writers, bufio.NewWriter(file))
	}
	return writers
}

func (table *SStable) CloseFiles(files []*os.File) {
	files[0].Close()
	// fmt.Println(files[0].Stat())
	files[1].Close()
	// fmt.Println(files[1].Stat())
	files[2].Close()
	// fmt.Println(files[2].Stat())
	files[3].Close()
	// fmt.Println(files[3].Stat())

}

func (table *SStable) AddRecord(counter int, offsetData uint64, offsetIndex uint64, record *record.Record,
	bf *bloomfilter.BloomFilter, merkle *merkle.MerkleTree, writers []*bufio.Writer) (uint64, uint64) {
	// Appending elements to BloomFilter and MerkleTree
	// fmt.Println("adddd", record)
	bf.Hash(record.GetKey())
	merkle.AddLeaf(record.Data)
	// First or every fifth record append to summary
	if counter == 1 || counter%5 == 0 {
		// Appending record to existing.bin -> temporary summary
		recordInsertSum := NewSummary(record.GetKey(), offsetIndex)
		// writers[3] -> summary bufio.Writer
		recordInsertSum.WriteSummary(writers[3])
	}

	// Appending record to data file
	// writers[0] -> data bufio.Writer
	WriteDataTable(record, writers[0])

	index := NewIndex(record.GetKey(), offsetData)
	index.WriteIndexTable(writers[1])

	// increase offset summary
	offsetIndex += index.GetSize()

	// increase offset index
	offsetData += record.GetSize()

	return offsetData, offsetIndex
}

func (table *SStable) EncodeHelpers(bf *bloomfilter.BloomFilter, merkle *merkle.MerkleTree) {
	bf.Encode(table.BloomFilterPath)
	merkle.GenerateMerkleTree()
	merkle.Encode()
	table.FormTOC()
}

func (table *SStable) EncodeHelpersWithoutTOC(bf *bloomfilter.BloomFilter, merkle *merkle.MerkleTree) {
	bf.Encode(table.BloomFilterPath)
	merkle.GenerateMerkleTree()
	merkle.Encode()
}

func NewSStableFromTOC(tocFilePath string) *SStable {
	file, err := os.Open(tocFilePath)
	if err != nil {
		fmt.Println("Error")
		return nil
	}
	scanner := bufio.NewScanner(file)
	var data []string
	for scanner.Scan() {

		data = append(data, scanner.Text())
	}

	if data[0] != "" {
		data = append(data, "")
	}
	return &SStable{
		DataTablePath:   data[0],
		IndexTablePath:  data[1],
		SummaryPath:     data[2],
		BloomFilterPath: data[3],
		MetaDataPath:    data[4],
		TOCFilePath:     tocFilePath,
		SStableFilePath: data[5]}
}

func (table *SStable) FormSStableTest(records *[]*record.Record) {
	files := table.CreateFiles()
	writers := table.CreateWriters(files)
	counter := 1
	offsetData := uint64(0)
	offsetIndex := uint64(0)
	bf := bloomfilter.NewBLoomFilter(100, 0.01)
	merkle := merkle.NewMerkleTreeFile(table.MetaDataPath)
	for _, record := range *records {
		offsetData, offsetIndex = table.AddRecord(counter, offsetData, offsetIndex, record, bf, merkle, writers)
		counter++
	}
	// fmt.Println("First and Last: ")
	first := (*records)[0]
	// fmt.Println("First -> ", first.String())
	last := (*records)[len((*records))-1]
	// fmt.Println("Last -> ", last.String())
	table.CopyExistingToSummary(first, last, files, writers)
	table.EncodeHelpers(bf, merkle)
	table.CloseFiles(files)

	// PrintSummary(table.SummaryPath)
	// PrintIndexTable(table.IndexTablePath)

	// PrintDataTable(table.DataTablePath)

}

func (table *SStable) FormTOC() {
	file, err := os.Create(table.TOCFilePath)
	if err != nil {
		fmt.Println("Error", err)
		return
	}
	defer file.Close()
	_, err = file.WriteString(table.DataTablePath + "\n" + table.IndexTablePath + "\n" + table.SummaryPath + "\n" + table.BloomFilterPath + "\n" + table.MetaDataPath + "\n" + table.SStableFilePath)
	if err != nil {
		fmt.Println("Error", err)
		return
	}
}

// func (table *SStable)

func (table *SStable) Search(key string) *record.Record {

	bf := bloomfilter.NewBLoomFilter(100, 0.01)
	bf.Decode(table.BloomFilterPath)
	if !bf.Find(key) {
		return nil
	}

	file, err := os.Open(table.SummaryPath)
	if err != nil {
		fmt.Println("Error", err)
		return nil
	}
	bytes := make([]byte, KEY_MIN_SIZE+KEY_MAX_SIZE)

	_, err = io.ReadAtLeast(file, bytes, KEY_MIN_SIZE+KEY_MAX_SIZE)
	if err != nil {
		return nil
	}
	keyMinSize := binary.BigEndian.Uint64(bytes[:KEY_MIN_SIZE])
	keyMaxSize := binary.BigEndian.Uint64(bytes[KEY_MIN_SIZE : KEY_MAX_SIZE+KEY_MIN_SIZE])
	keyMin := record.ReadKey(file, keyMinSize)
	keyMax := record.ReadKey(file, keyMaxSize)

	if key >= keyMin && key <= keyMax {

		sumRec1, err := ReadSummary(file)
		if err != nil {
			return nil
		}
		for {

			sumRec2, err := ReadSummary(file)
			if err != nil && err != io.EOF {
				return nil
			}
			if err == io.EOF {
				return table.searchRecord(key, sumRec1.GetOffsetSum())
			}
			if key == sumRec1.GetKey() {
				return table.searchRecord(key, sumRec1.GetOffsetSum())
			}
			if key == sumRec2.GetKey() {
				return table.searchRecord(key, sumRec2.GetOffsetSum())
			}
			if key > sumRec1.GetKey() && key < sumRec2.GetKey() {
				return table.searchRecord(key, sumRec1.GetOffsetSum())
			}
			sumRec1 = sumRec2

		}

	} else {
		return nil
	}
}

func (table *SStable) searchRecord(key string, offset uint64) *record.Record {
	file, err := os.Open(table.IndexTablePath)
	if err != nil {
		fmt.Println("Error", err)
		return nil
	}
	file.Seek(int64(offset), 0)
	for {
		index, err := ReadIndexRecord(file)
		if err != nil {
			fmt.Println("Error", err)
			return nil
		}
		if index.GetKey() == key {

			fileData, err := os.Open(table.DataTablePath)
			if err != nil {
				fmt.Println("Error", err)
				return nil
			}
			fileData.Seek(int64(index.GetOffset()), 0)
			record, err := record.ReadRecord(fileData)
			if err != nil {
				fmt.Println("Error", err)
				return nil
			}
			return record
		}

	}

}

func searchDataRange(keyRange1 string, keyRange2 string, file *os.File, numRecords uint64) []record.Record {
	records := make([]record.Record, 0)
	i := 1
	for {
		if i >= int(numRecords) {
			return records
		}
		record, err := record.ReadRecord(file)
		if err == io.EOF {
			return records
		}
		if err != nil {
			fmt.Println("Error with read data record:", err)
			return nil
		}
		if keyRange1 <= record.GetKey() && record.GetKey() <= keyRange2 {
			records = append(records, *record)
			i++
			continue
		}
		if record.GetKey() > keyRange2 {
			return records
		}

	}
}

func (table *SStable) SearchRangeMultiple(keyRange1 string, keyRange2 string, numRecords uint64) []record.Record {

	file, err := os.Open(table.SummaryPath)
	if err != nil {
		fmt.Println("Error", err)
		return nil
	}
	bytes := make([]byte, KEY_MIN_SIZE+KEY_MAX_SIZE)

	_, err = io.ReadAtLeast(file, bytes, KEY_MIN_SIZE+KEY_MAX_SIZE)
	if err != nil {
		return nil
	}
	keyMinSize := binary.BigEndian.Uint64(bytes[:KEY_MIN_SIZE])
	keyMaxSize := binary.BigEndian.Uint64(bytes[KEY_MIN_SIZE : KEY_MAX_SIZE+KEY_MIN_SIZE])
	keyMin := record.ReadKey(file, keyMinSize)
	keyMax := record.ReadKey(file, keyMaxSize)
	if keyRange1 > keyMax || keyRange2 < keyMin {
		fmt.Println("Nepravilan opseg")
		return nil
	} else {

		fileData, err := os.Open(table.DataTablePath)
		if err != nil {
			fmt.Println("Error with open data file:", err)
			return nil
		}
		recordsRange := searchDataRange(keyRange1, keyRange2, fileData, numRecords)
		return recordsRange
	}
}

func searchDataPrefix(key string, file *os.File, numRec uint64) []record.Record {
	records := make([]record.Record, 0)
	i := 0
	for {
		if i >= int(numRec) {
			return records
		}
		record, err := record.ReadRecord(file)
		if err == io.EOF {
			return records
		}
		if err != nil {
			fmt.Println("Error with read data", err)
			return nil
		}
		if strings.HasPrefix(record.GetKey(), key) {
			records = append(records, *record)
			i++
			continue

		}
	}

}

func (table *SStable) SearchPrefixMultiple(key string, numRecords uint64) []record.Record {
	file, err := os.Open(table.SummaryPath)
	if err != nil {
		fmt.Println("Error", err)
		return nil
	}
	bytes := make([]byte, KEY_MIN_SIZE+KEY_MAX_SIZE)

	_, err = io.ReadAtLeast(file, bytes, KEY_MIN_SIZE+KEY_MAX_SIZE)
	if err != nil {
		return nil
	}
	keyMinSize := binary.BigEndian.Uint64(bytes[:KEY_MIN_SIZE])
	keyMaxSize := binary.BigEndian.Uint64(bytes[KEY_MIN_SIZE : KEY_MAX_SIZE+KEY_MIN_SIZE])
	keyMin := record.ReadKey(file, keyMinSize)
	keyMax := record.ReadKey(file, keyMaxSize)

	if (keyMin <= key && key <= keyMax) || strings.HasPrefix(keyMin, key) || strings.HasPrefix(keyMax, key) {
		fileData, err := os.Open(table.DataTablePath)
		if err != nil {
			fmt.Println("Error with open data file:", err)
			return nil
		}
		records := searchDataPrefix(key, fileData, numRecords)
		return records
	} else {
		fmt.Println("Neuspesna pretraga")
		return nil
	}
}

func (table *SStable) CreateExistingFiles() []*os.File {
	files := make([]*os.File, 0)
	file, err := os.Create(EXISTING_DATA)
	if err != nil {
		fmt.Println("Error data file")
		panic(err)
	}
	files = append(files, file)

	file, err = os.Create(EXISTING_INDEX)
	if err != nil {
		fmt.Println("Error index file")
		panic(err)
	}
	files = append(files, file)

	file, err = os.Create(EXISTING_SUMMARY)
	if err != nil {
		fmt.Println("Error summary file")
		panic(err)
	}
	files = append(files, file)

	file, err = os.Create(EXISTING)
	if err != nil {
		fmt.Println("Error existing.bin file")
		panic(err)
	}
	files = append(files, file)
	file, err = os.Create(table.SStableFilePath)
	if err != nil {
		fmt.Println("Error", err)
		panic(err)
	}
	files = append(files, file)

	return files
}

func (table *SStable) CalculateFileSizes(files []*os.File) (a uint64, b uint64) {
	fileBloom, err := os.Open(EXISTING_BLOOM)
	if err != nil {
		fmt.Println("Bloom error", err)
		return
	}
	defer fileBloom.Close()

	stat, err := fileBloom.Stat()
	if err != nil {
		fmt.Println("Stat bloom error", err)
		return
	}
	bloomFileSize := stat.Size()

	stat, err = files[2].Stat()
	if err != nil {
		fmt.Println("Stat summary error", err)
		return
	}
	summarySize := stat.Size()

	return uint64(bloomFileSize), uint64(summarySize)

}

func copyAndDelete(file *os.File, sourceFile *os.File) {
	sourceFile.Seek(0, 0)

	_, err := io.Copy(file, sourceFile)
	if err != nil {
		fmt.Println("Error", err)
		return
	}
	sourceFile.Close()
	err = os.Remove(sourceFile.Name())
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
}
func (table *SStable) CopyAllandWriteHeader(sizes []uint64, files []*os.File, writers []*bufio.Writer) {
	data := make([]byte, 0)
	data = binary.BigEndian.AppendUint64(data, uint64(sizes[0]))
	data = binary.BigEndian.AppendUint64(data, uint64(sizes[1]))
	data = binary.BigEndian.AppendUint64(data, uint64(sizes[2]))
	err := binary.Write(writers[4], binary.BigEndian, data)
	if err != nil {
		panic(err)
	}
	writers[4].Flush()

	fileBloom, err := os.Open(EXISTING_BLOOM)
	if err != nil {
		fmt.Println("Bloom error", err)
		return
	}
	defer fileBloom.Close()

	copyAndDelete(files[4], fileBloom)
	copyAndDelete(files[4], files[2])
	copyAndDelete(files[4], files[1])
	copyAndDelete(files[4], files[0])
}
func (table *SStable) EncodeHelpersOneFile(bf *bloomfilter.BloomFilter, merkle *merkle.MerkleTree) {
	bf.Encode(EXISTING_BLOOM)
	merkle.GenerateMerkleTree()
	merkle.Encode()
	table.FormTOC()
}
func (table *SStable) CopyExistingToSummary(first *record.Record, last *record.Record, files []*os.File, writers []*bufio.Writer) {
	files[3].Seek(0, 0)
	// creating header for real summary
	summary := NewSummaryHeader(first.GetKey(), last.GetKey())
	// writers[2] -> real summary bufio.Writer
	summary.WriteSummaryHeader(writers[2])

	// copying existing.bin to real summary
	_, err := io.Copy(files[2], files[3])
	if err != nil {
		fmt.Println("Error existing to summary", err)
		return
	}
	files[3].Close()

	// deleting existing.bin
	err = os.Remove("existing.bin")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

}

func (table *SStable) FormSStableOneFile(records *[]*record.Record) {
	files := table.CreateExistingFiles()
	writers := table.CreateWriters(files)
	counter := 1
	offsetData := uint64(0)
	offsetIndex := uint64(0)
	bf := bloomfilter.NewBLoomFilter(100, 0.01)
	merkle := merkle.NewMerkleTreeFile(table.MetaDataPath)
	for _, record := range *records {
		offsetData, offsetIndex = table.AddRecord(counter, offsetData, offsetIndex, record, bf, merkle, writers)
		counter++
	}

	first := (*records)[0]
	last := (*records)[len((*records))-1]

	table.CopyExistingToSummary(first, last, files, writers)
	table.EncodeHelpersOneFile(bf, merkle)

	bloomSize, summarySize := table.CalculateFileSizes(files)
	sizes := []uint64{bloomSize, summarySize, offsetIndex}

	table.CopyAllandWriteHeader(sizes, files, writers)
	table.CloseFiles(files)

	// table.PrintSStable()
}
func (table *SStable) ReadSStableHeader(file *os.File) (uint64, uint64, uint64) {
	bytes := make([]byte, HEADER)
	_, err := io.ReadAtLeast(file, bytes, HEADER)
	ret := uint64(0)
	if err != nil {
		fmt.Println("Error with read", err)
		return ret, ret, ret
	}
	bloomSize := binary.BigEndian.Uint64(bytes[:KEY_SIZE])
	sumSize := binary.BigEndian.Uint64(bytes[KEY_SIZE : KEY_SIZE+KEY_SIZE])
	indexSize := binary.BigEndian.Uint64(bytes[KEY_SIZE+KEY_SIZE : KEY_SIZE+KEY_SIZE+KEY_SIZE])
	return bloomSize, sumSize, indexSize
}

func (table *SStable) PrintSStable() {
	file, err := os.Open(table.SStableFilePath)
	if err != nil {
		fmt.Println("Error open sstable", err)
		return
	}
	defer file.Close()
	bloomSize, sumSize, indexSize := table.ReadSStableHeader(file)

	file.Seek(int64(bloomSize)+HEADER, 0)

	sumHeader, err := ReadSumarryHeader(file)
	if err != nil {
		fmt.Println("Error with summary header", err)
		return
	}

	fmt.Println(sumHeader)
	fmt.Println("=============== Summary ===============")
	for {
		currentPos, err := file.Seek(0, os.SEEK_CUR)
		if err != nil {
			fmt.Println("Error current position", err)
			return
		}

		if bloomSize+sumSize+HEADER <= uint64(currentPos) {
			break
		}
		sumRecord, err := ReadSummary(file)
		if err != nil {
			fmt.Println("Error with read summary", err)
			return
		}
		fmt.Println(sumRecord)

	}
	fmt.Println("=============== IndexTable =============== ")
	for {
		currentPos, err := file.Seek(0, os.SEEK_CUR)
		if err != nil {
			fmt.Println("Error current position", err)
			return
		}
		if HEADER+bloomSize+sumSize+indexSize <= uint64(currentPos) {
			break
		}
		indexRecord, err := ReadIndexRecord(file)
		if err != nil {
			fmt.Println("Error with read index", err)
			return
		}
		fmt.Println(indexRecord)

	}
	for {
		dataRecord, err := record.ReadRecord(file)
		if err != nil {
			return
		}
		fmt.Println(dataRecord)
	}

}

func (table *SStable) SearchOneFile(key string) *record.Record {
	file, err := os.Open(table.SStableFilePath)
	if err != nil {
		fmt.Println("Error open sstable", err)
		return nil
	}
	defer file.Close()
	bloomSize, sumSize, indexSize := table.ReadSStableHeader(file)

	bf := bloomfilter.BloomFilter{}
	bf.DecoderSSOneFile(file)
	if !bf.Find(key) {
		return nil
	}
	file.Seek(int64(bloomSize)+HEADER, 0)

	bytes := make([]byte, KEY_MIN_SIZE+KEY_MAX_SIZE)

	_, err = io.ReadAtLeast(file, bytes, KEY_MIN_SIZE+KEY_MAX_SIZE)
	if err != nil {
		return nil
	}
	keyMinSize := binary.BigEndian.Uint64(bytes[:KEY_MIN_SIZE])
	keyMaxSize := binary.BigEndian.Uint64(bytes[KEY_MIN_SIZE : KEY_MAX_SIZE+KEY_MIN_SIZE])
	keyMin := record.ReadKey(file, keyMinSize)
	keyMax := record.ReadKey(file, keyMaxSize)

	if key >= keyMin && key <= keyMax {

		sumRec1, err := ReadSummary(file)
		if err != nil {
			return nil
		}
		for {

			sumRec2, err := ReadSummary(file)
			if err != nil && err != io.EOF {
				return nil
			}
			if err == io.EOF {
				return table.searchRecordOneFile(file, key, sumRec1.GetOffsetSum(), bloomSize, sumSize, indexSize)
			}
			if key == sumRec1.GetKey() {
				return table.searchRecordOneFile(file, key, sumRec1.GetOffsetSum(), bloomSize, sumSize, indexSize)
			}
			if key == sumRec2.GetKey() {
				return table.searchRecordOneFile(file, key, sumRec2.GetOffsetSum(), bloomSize, sumSize, indexSize)
			}
			if key > sumRec1.GetKey() && key < sumRec2.GetKey() {
				return table.searchRecordOneFile(file, key, sumRec1.GetOffsetSum(), bloomSize, sumSize, indexSize)
			}
			sumRec1 = sumRec2

		}

	} else {
		return nil
	}
}

func (table *SStable) searchRecordOneFile(file *os.File, key string, offset uint64, bloomSize uint64, sumSize uint64, indexSize uint64) *record.Record {

	file.Seek(int64(HEADER+bloomSize+sumSize+offset), 0)
	for {
		index, err := ReadIndexRecord(file)
		if err != nil {
			fmt.Println("Error", err)
			return nil
		}
		if index.GetKey() == key {

			file.Seek(int64(HEADER+bloomSize+sumSize+indexSize+index.GetOffset()), 0)
			record, err := record.ReadRecord(file)
			if err != nil {
				fmt.Println("Error", err)
				return nil
			}
			return record
		}

	}

}

func (table *SStable) SearchRangeSingle(keyRange1 string, keyRange2 string, numRecords uint64) []record.Record {

	file, err := os.Open(table.SStableFilePath)
	if err != nil {
		fmt.Println("Error", err)
		return nil
	}
	bloomSize, sumSize, indexSize := table.ReadSStableHeader(file)
	file.Seek(int64(bloomSize)+HEADER, 0)

	bytes := make([]byte, KEY_MIN_SIZE+KEY_MAX_SIZE)

	_, err = io.ReadAtLeast(file, bytes, KEY_MIN_SIZE+KEY_MAX_SIZE)
	if err != nil {
		return nil
	}
	keyMinSize := binary.BigEndian.Uint64(bytes[:KEY_MIN_SIZE])
	keyMaxSize := binary.BigEndian.Uint64(bytes[KEY_MIN_SIZE : KEY_MAX_SIZE+KEY_MIN_SIZE])
	keyMin := record.ReadKey(file, keyMinSize)
	keyMax := record.ReadKey(file, keyMaxSize)

	if keyRange1 > keyMax || keyRange2 < keyMin {
		fmt.Println("Nepravilan opseg")
		return nil
	} else {
		file.Seek(int64(HEADER+bloomSize+sumSize+indexSize), 0)
		recordsRange := searchDataRange(keyRange1, keyRange2, file, numRecords)
		return recordsRange
	}
}

func (table *SStable) SearchPrefixSingle(key string, numRecords uint64) []record.Record {
	file, err := os.Open(table.SStableFilePath)
	if err != nil {
		fmt.Println("Error", err)
		return nil
	}
	bloomSize, sumSize, indexSize := table.ReadSStableHeader(file)
	file.Seek(int64(bloomSize)+HEADER, 0)

	bytes := make([]byte, KEY_MIN_SIZE+KEY_MAX_SIZE)

	_, err = io.ReadAtLeast(file, bytes, KEY_MIN_SIZE+KEY_MAX_SIZE)
	if err != nil {
		return nil
	}
	keyMinSize := binary.BigEndian.Uint64(bytes[:KEY_MIN_SIZE])
	keyMaxSize := binary.BigEndian.Uint64(bytes[KEY_MIN_SIZE : KEY_MAX_SIZE+KEY_MIN_SIZE])
	keyMin := record.ReadKey(file, keyMinSize)
	keyMax := record.ReadKey(file, keyMaxSize)

	if (keyMin <= key && key <= keyMax) || strings.HasPrefix(keyMin, key) || strings.HasPrefix(keyMax, key) {

		file.Seek(int64(HEADER+bloomSize+sumSize+indexSize), 0)
		records := searchDataPrefix(key, file, numRecords)
		return records
	} else {
		fmt.Println("Neuspesna pretraga")
		return nil
	}
}
