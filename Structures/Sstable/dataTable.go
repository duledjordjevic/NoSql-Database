package sstable

import (
	record "NAiSP/Structures/Record"
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
)

func WriteDataTable(rec record.Record, writer *bufio.Writer) {

	err := binary.Write(writer, binary.BigEndian, rec.Data)
	if err != nil {
		panic(err)
	}

	writer.Flush()
}

func PrintDataTable(dataFilePath string) {

	file, err := os.Open(dataFilePath)
	if err != nil {
		panic(err)
	}
	fmt.Println("=============== Data Table ===============")
	i := 1
	for {
		recordForPrint, err := record.ReadRecord(file)
		if err != nil {
			return
		}
		fmt.Println("Record ", i)
		fmt.Println(recordForPrint.String())
		i += 1
	}
}
