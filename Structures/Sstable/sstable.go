package sstable

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	"NAiSP/Structures/record"
	"bufio"
	"fmt"
	"io"
	"os"
)

type SStable struct {
	DataTablePath   string
	IndexTablePath  string
	SummaryPath     string
	BloomFilterPath string
}

func NewSStable(dataTable string, indexTable string, summary string, bloom string) *SStable {
	sstable := SStable{DataTablePath: dataTable, IndexTablePath: indexTable, SummaryPath: summary, BloomFilterPath: bloom}
	return &sstable
}

func (table *SStable) FormDataIndexSummary(records []record.Record) {
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
	for _, record := range records {
		bf.Hash(record.GetKey())
		if i == 1 {
			firstRecord = record
		}
		if i == len(records) {
			lastRecord = record
		}
		WriteDataTable(record, writer)

		index := NewIndex(record.GetKey(), uint64(currentSize))
		index.WriteIndexTable(writerIndex)
		currentSize += record.GetSize()
		currentOffIndex += index.GetSize()
		if i%5 == 0 {
			recordForSummary = record

			recordInsertSum := NewSummary(recordForSummary.GetKey(), currentOffIndex)
			recordInsertSum.WriteSummary(writerSumEx)
		}
		i++
	}
	bf.Encode(table.BloomFilterPath)
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
