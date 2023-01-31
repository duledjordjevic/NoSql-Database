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
		WriteDataTable(*record, writer)

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

// func (table *SStable) AddRecord(i int, record record.Record, fileData *os.File, fileIndex *os.File, fileSum *os.File, merkle *merkle.MerkleTree, bf *bloomfilter.BloomFilter) {

// }
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
