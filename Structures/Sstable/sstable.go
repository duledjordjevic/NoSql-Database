package sstable

import (
	"NAiSP/Structures/record"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

type SStable struct {
	DataTablePath  string
	IndexTablePath string
}

func NewSStable(dataTable string, indexTable string) *SStable {
	sstable := SStable{DataTablePath: dataTable, IndexTablePath: indexTable}
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

	writer := bufio.NewWriter(file)
	writerIndex := bufio.NewWriter(fileIndex)
	i := 1
	for _, record := range records {
		// if i == 1 {
		// 	firstRecord := record
		// }
		WriteDataTable(record, writer)
		NumOffset, check := getOffsetForKey(table.DataTablePath, record.GetKey())
		if !check {
			continue
		}
		var offset bytes.Buffer
		binary.Write(&offset, binary.BigEndian, NumOffset)
		index := NewIndex(record.GetKey(), offset.Bytes())
		index.WriteIndexTable(writerIndex)
		i++
	}

	// PrintDataTable(table.DataTablePath)
	PrintIndexTable(table.IndexTablePath)
}
