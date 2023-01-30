package sstable

import (
	"NAiSP/Structures/record"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type SStable struct {
	DataTablePath  string
	IndexTablePath string
	SummaryPath    string
}

func NewSStable(dataTable string, indexTable string, summary string) *SStable {
	sstable := SStable{DataTablePath: dataTable, IndexTablePath: indexTable, SummaryPath: summary}
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
	for _, record := range records {
		if i == 1 {
			firstRecord = record
		}
		if i == len(records) {
			lastRecord = record
		}
		WriteDataTable(record, writer)
		NumOffset, check := getOffsetForKey(table.DataTablePath, record.GetKey())
		if !check {
			continue
		}
		var offset bytes.Buffer
		binary.Write(&offset, binary.BigEndian, NumOffset)
		index := NewIndex(record.GetKey(), offset.Bytes())
		index.WriteIndexTable(writerIndex)
		if i%10 == 0 {
			recordForSummary = record
			OffIndex, check := getOffsetForIndexKey(table.IndexTablePath, recordForSummary.GetKey())
			if !check {
				fmt.Println("Doslo je do greske")
				return
			}
			var offset bytes.Buffer
			binary.Write(&offset, binary.BigEndian, OffIndex)
			recordInsertSum := NewSummary(recordForSummary.GetKey(), offset.Bytes())
			recordInsertSum.WriteSummary(writerSumEx)
		}
		i++
	}

	sum := NewSummaryHeader(firstRecord.GetKey(), lastRecord.GetKey())
	// str := sum.String()
	// fmt.Println(str)
	sum.WriteSummary(writerSum)

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
	// PrintIndexTable(table.IndexTablePath)
}
