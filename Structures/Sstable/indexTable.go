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
	OFFSIZE  = 8

	KEYSIZESTART  = 0
	OFFSIZESTSART = KEYSIZESTART + KEY_SIZE
	KEYSTART      = OFFSIZESTSART + OFFSIZE
	OFFSTART      = KEYSTART + KEY_SIZE
)

type IndexTable struct {
	Data []byte
}

func (indexRecord *IndexTable) GetKeySize() uint64 {
	return binary.BigEndian.Uint64(indexRecord.Data[:KEY_SIZE])
}
func (indexRecord *IndexTable) GetOffSize() uint64 {
	return binary.BigEndian.Uint64(indexRecord.Data[OFFSIZESTSART:OFFSTART])
}
func (indexRecord *IndexTable) GetKey() string {
	keySize := indexRecord.GetKeySize()
	return string(indexRecord.Data[KEYSTART : KEYSTART+keySize])
}
func (indexRecord *IndexTable) GetOffset() []byte {
	keySize := indexRecord.GetKeySize()
	return indexRecord.Data[KEYSTART+keySize:]
}

func NewIndex(key string, offset []byte) *IndexTable {
	data := make([]byte, 0)
	data = binary.BigEndian.AppendUint64(data, uint64(len(key)))
	// offsize := make([]byte, binary.MaxVarintLen64)
	data = binary.BigEndian.AppendUint64(data, uint64(len(offset)))
	// data = binary.BigEndian.AppendUint64(data, uint64(n))
	data = append(data, []byte(key)...)
	data = append(data, offset...)
	return &IndexTable{Data: data}

}

func (index *IndexTable) GetSize() uint64 {
	return index.GetOffSize() + index.GetKeySize() + KEY_SIZE + OFFSIZE
}
func (index *IndexTable) WriteIndexTable(writer *bufio.Writer) {
	err := binary.Write(writer, binary.BigEndian, index.Data)
	if err != nil {
		panic(err)
	}
	writer.Flush()

}

func getOffsetForKey(dataFilePath string, key string) (uint64, bool) {
	file, err := os.Open(dataFilePath)
	if err != nil {
		return 0, false
	}

	defer file.Close()
	current := uint64(0)
	for {
		recordOff, err := record.ReadRecord(file)
		if err != nil {
			return 0, false
		}
		if recordOff.GetKey() == key {
			return current, true
		}
		current += recordOff.GetSize()

	}

}
func ReadOffset(file *os.File, keySize uint64) uint64 {
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
	offSize := binary.BigEndian.Uint64(bytes[KEY_SIZE:KEYSTART])
	key := record.ReadKey(file, keySize)
	offset := record.ReadValue(file, offSize)
	indexRecord := NewIndex(key, offset)

	return indexRecord, nil

}
func (index *IndexTable) String() string {
	str := ""
	str += strconv.FormatUint((index.GetKeySize()), 10) + " "
	str += strconv.FormatUint((index.GetOffSize()), 10) + " "
	str += index.GetKey() + " "
	for _, v := range index.GetOffset() {
		str += strconv.Itoa(int(v))
	}
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
		fmt.Println("Record ", i)
		str := indexForPrint.String()
		fmt.Print(str)
		i += 1
	}
}
