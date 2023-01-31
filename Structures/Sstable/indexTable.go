package sstable

import (
	"NAiSP/Structures/record"
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

const (
	KEY_SIZE = 8

	KEYSTART = KEY_SIZE
	OFFSIZE  = 8
)

type IndexTable struct {
	Data []byte
}

func (indexRecord *IndexTable) GetKeySize() uint64 {
	return binary.BigEndian.Uint64(indexRecord.Data[:KEY_SIZE])
}

//	func (indexRecord *IndexTable) GetOffSize() uint64 {
//		return binary.BigEndian.Uint64(indexRecord.Data[OFFSIZESTSART:OFFSTART])
//	}
func (indexRecord *IndexTable) GetKey() string {
	keySize := indexRecord.GetKeySize()
	return string(indexRecord.Data[KEY_SIZE : KEY_SIZE+keySize])
}
func (indexRecord *IndexTable) GetOffset() uint64 {
	keySize := indexRecord.GetKeySize()
	return binary.BigEndian.Uint64((indexRecord.Data[KEY_SIZE+keySize:]))
}

func NewIndex(key string, offset uint64) *IndexTable {
	data := make([]byte, 0)
	data = binary.BigEndian.AppendUint64(data, uint64(len(key)))
	// offsize := make([]byte, binary.MaxVarintLen64)
	// data = binary.BigEndian.AppendUint64(data, uint64(n))
	data = append(data, []byte(key)...)
	data = binary.BigEndian.AppendUint64(data, uint64(offset))
	return &IndexTable{Data: data}

}

func (index *IndexTable) GetSize() uint64 {
	return index.GetKeySize() + KEY_SIZE + OFFSIZE
}
func (index *IndexTable) WriteIndexTable(writer *bufio.Writer) {
	err := binary.Write(writer, binary.BigEndian, index.Data)
	if err != nil {
		panic(err)
	}
	writer.Flush()

}
func ReadOffset(file *os.File) uint64 {
	bytes := make([]byte, 8)
	_, err := io.ReadAtLeast(file, bytes, 8)
	if err != nil {
		fmt.Println("Greska kod citanja Key-a")
		log.Fatal(err)
	}
	return binary.BigEndian.Uint64(bytes)
}

func ReadIndexRecord(file *os.File) (*IndexTable, error) {

	bytes := make([]byte, KEYSTART)
	_, err := io.ReadAtLeast(file, bytes, KEYSTART)
	if err != nil {
		return nil, err
	}
	keySize := binary.BigEndian.Uint64(bytes[:KEY_SIZE])
	key := record.ReadKey(file, keySize)
	offset := ReadOffset(file)
	indexRecord := NewIndex(key, offset)

	return indexRecord, nil

}
func (index *IndexTable) String() string {
	str := ""
	str += strconv.FormatUint((index.GetKeySize()), 10) + " "
	str += index.GetKey() + " " + fmt.Sprint(index.GetOffset())
	str += "\n"
	return str
}

func PrintIndexTable(indexTablePath string) {
	file, err := os.Open(indexTablePath)
	if err != nil {
		panic(err)
	}

	fmt.Println("=============== IndexTable =============== ")
	i := 1
	for {
		indexForPrint, err := ReadIndexRecord(file)
		if err != nil {
			return
		}
		str := indexForPrint.String()
		fmt.Print(str)
		i += 1
	}
}
