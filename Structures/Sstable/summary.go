package sstable

import (
	"NAiSP/Structures/record"
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
)

const (
	KEY_MIN_SIZE  = 8
	KEY_MAX_SIZE  = 8
	KEY_MIN_START = KEY_MIN_SIZE + KEY_MAX_SIZE
)

type Summary struct {
	Data []byte
}

func (sum *Summary) GetKeyMinSize() uint64 {
	return binary.BigEndian.Uint64(sum.Data[:KEY_MIN_SIZE])
}

func (sum *Summary) GetKeyMaxSize() uint64 {
	return binary.BigEndian.Uint64(sum.Data[KEY_MIN_SIZE : KEY_MAX_SIZE+KEY_MIN_SIZE])
}

func (sum *Summary) GetKeyMin() string {
	keyMinSize := sum.GetKeyMinSize()
	return string(sum.Data[KEY_MIN_START : KEY_MIN_START+keyMinSize])
}

func (sum *Summary) GetKeyMax() string {
	keyMinSize := sum.GetKeyMinSize()
	return string(sum.Data[KEY_MIN_START+keyMinSize:])
}

func NewSummary(keyMin string, keyMax string) *Summary {
	data := make([]byte, 0)
	data = binary.BigEndian.AppendUint64(data, uint64(len(keyMin)))
	data = binary.BigEndian.AppendUint64(data, uint64(len(keyMax)))
	data = append(data, []byte(keyMin)...)
	data = append(data, []byte(keyMax)...)
	return &Summary{Data: data}
}

func (sum *Summary) WriteSummary(writer *bufio.Writer) {
	err := binary.Write(writer, binary.BigEndian, sum.Data)
	if err != nil {
		fmt.Println("Los unos")
		return
	}
	writer.Flush()
}

func ReadSumarry(file *os.File) (*Summary, error) {

	bytes := make([]byte, KEY_MIN_SIZE+KEY_MAX_SIZE)
	_, err := io.ReadAtLeast(file, bytes, KEY_MIN_SIZE+KEY_MAX_SIZE)
	if err != nil {
		return nil, err
	}
	keyMinSize := binary.BigEndian.Uint64(bytes[:KEY_MIN_SIZE])
	keyMaxSize := binary.BigEndian.Uint64(bytes[KEY_MIN_SIZE : KEY_MAX_SIZE+KEY_MIN_SIZE])
	keyMin := record.ReadKey(file, keyMinSize)
	keyMax := record.ReadKey(file, keyMaxSize)
	sumRecord := NewSummary(keyMin, keyMax)

	return sumRecord, nil

}
func (sum *Summary) String() string {
	str := ""
	str += strconv.FormatUint((sum.GetKeyMinSize()), 10) + " "
	str += strconv.FormatUint((sum.GetKeyMaxSize()), 10) + " "
	str += sum.GetKeyMin() + " " + sum.GetKeyMax()

	str += "\n"
	return str
}

func PrintSummary(summaryPath string) {
	file, err := os.Open(summaryPath)
	if err != nil {
		fmt.Println("Error")
		return
	}
	defer file.Close()

	sum, err := ReadSumarry(file)
	if err != nil {
		fmt.Println("Error")
		return
	}
	str := sum.String()
	fmt.Println(str)

}
