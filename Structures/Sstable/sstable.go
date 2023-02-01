package sstable

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	merkle "NAiSP/Structures/Merkle"
	record "NAiSP/Structures/Record"
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	EXISTING = "existing.bin"
)

type SStable struct {
	DataTablePath   string
	IndexTablePath  string
	SummaryPath     string
	BloomFilterPath string
	MetaDataPath    string
	TOCFilePath     string
}

func NewSStable(dataTable string, indexTable string, summary string, bloom string, meta string, tocPath string) *SStable {
	sstable := SStable{DataTablePath: dataTable, IndexTablePath: indexTable, SummaryPath: summary, BloomFilterPath: bloom, MetaDataPath: meta, TOCFilePath: tocPath}
	return &sstable
}
func NewSStableAutomatic(prefix, sufix string) *SStable {
	sstable := SStable{DataTablePath: prefix + "data" + sufix + ".bin",
		IndexTablePath:  prefix + "index" + sufix + ".bin",
		SummaryPath:     prefix + "summary" + sufix + ".bin",
		BloomFilterPath: prefix + "bloomfilter" + sufix + ".gob",
		MetaDataPath:    prefix + "Metadata" + sufix + ".txt",
		TOCFilePath:     prefix + "TOC" + sufix + ".txt"}
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
	PrintSummary(table.SummaryPath)
	// PrintDataTable(table.DataTablePath)
	PrintIndexTable(table.IndexTablePath)
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
	for _, file := range files {
		if file.Name() == EXISTING {
			return
		}
		file.Close()
	}
}

func (table *SStable) AddRecord(counter int, offsetData uint64, offsetIndex uint64, record *record.Record,
	bf *bloomfilter.BloomFilter, merkle *merkle.MerkleTree, writers []*bufio.Writer) (uint64, uint64) {
	// Appending elements to BloomFilter and MerkleTree
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

func (table *SStable) CopyExistingToSummary(first *record.Record, last *record.Record, files []*os.File, writers []*bufio.Writer) {
	files[3].Seek(0, 0)
	// creating header for real summary
	summary := NewSummaryHeader(first.GetKey(), last.GetKey())
	// writers[2] -> real summary bufio.Writer
	summary.WriteSummaryHeader(writers[2])

	// copying existing.bin to real summary
	_, err := io.Copy(files[2], files[3])
	if err != nil {
		fmt.Println("Error")
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

	return &SStable{DataTablePath: data[0], IndexTablePath: data[1], SummaryPath: data[2], BloomFilterPath: data[3], MetaDataPath: data[4], TOCFilePath: tocFilePath}
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
	fmt.Println("First and Last: ")
	first := (*records)[0]
	fmt.Println("First -> ", first.String())
	last := (*records)[len((*records))-1]
	fmt.Println("Last -> ", last.String())
	table.CopyExistingToSummary(first, last, files, writers)
	table.EncodeHelpers(bf, merkle)
	table.CloseFiles(files)

	PrintIndexTable(table.IndexTablePath)
	PrintSummary(table.SummaryPath)
	// PrintDataTable(table.DataTablePath)

}

func (table *SStable) FormTOC() {
	file, err := os.Create(table.TOCFilePath)
	if err != nil {
		fmt.Println("Error")
		return
	}
	defer file.Close()
	_, err = file.WriteString(table.DataTablePath + "\n" + table.IndexTablePath + "\n" + table.SummaryPath + "\n" + table.BloomFilterPath + "\n" + table.MetaDataPath)
	if err != nil {
		fmt.Println("Error", err)
		return
	}
}

func (table *SStable) Search(key string) *record.Record {
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
		bf := bloomfilter.NewBLoomFilter(100, 0.01)
		bf.Decode(table.BloomFilterPath)
		if !bf.Find(key) {
			return nil
		}

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
