package record

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

type Record struct {
	Data []byte
}

// KONSTRUKTOR ZA RECORD KADA SE PRVI PUT PRAVI
func NewRecordKeyValue(key string, value []byte, tombstone byte) *Record {
	data := make([]byte, 0)

	data = binary.BigEndian.AppendUint32(data, CRC32(append([]byte(key), value...)))
	data = binary.BigEndian.AppendUint64(data, uint64(time.Now().UTC().UnixNano()/1e6))
	data = append(data, tombstone)
	data = binary.BigEndian.AppendUint64(data, uint64(len(key)))
	data = binary.BigEndian.AppendUint64(data, uint64(len(value)))
	data = append(data, []byte(key)...)
	data = append(data, value...)

	return &Record{Data: data}
}

// KONSTRUKTOR ZA RECORD KADA SE PROCITA IZ FAJLA
func NewRecordByte(data []byte) *Record {
	return &Record{Data: data}
}

// get funckije dobavljaju vrednosti iz niza bajtova

func (rec *Record) GetCRC() uint32 {
	return binary.BigEndian.Uint32(rec.Data[:CRC_SIZE])
}

func (rec *Record) GetTimeStamp() uint64 {
	return binary.BigEndian.Uint64(rec.Data[TIMESTAMP_START:TOMBSTONE_START])
}

func (rec *Record) GetTombStone() byte {
	return rec.Data[TOMBSTONE_START]
}

func (rec *Record) GetKeySize() uint64 {
	return binary.BigEndian.Uint64(rec.Data[KEY_SIZE_START:VALUE_SIZE_START])
}

func (rec *Record) GetValueSize() uint64 {
	return binary.BigEndian.Uint64(rec.Data[VALUE_SIZE_START:KEY_START])
}

func (rec *Record) GetKey() string {
	keySize := rec.GetKeySize()
	return string(rec.Data[KEY_START : KEY_START+keySize])
}

func (rec *Record) GetValue() []byte {
	keySize := rec.GetKeySize()
	return rec.Data[KEY_START+keySize:]
}

func (rec *Record) GetSize() uint64 {
	return uint64(4 + 8 + 1 + 8 + 8 + rec.GetKeySize() + rec.GetValueSize())
}
func (rec *Record) CheckCRC() bool {
	CRC := rec.GetCRC()
	if CRC == CRC32(rec.Data[KEY_START:]) {
		return true
	}
	return false
}

func (rec *Record) GreaterThan(other *Record) bool {
	if rec.GetTimeStamp() > other.GetTimeStamp() {
		return true
	}
	return false
}

func (rec *Record) String() string {
	str := " "
	str += strconv.FormatUint(uint64(rec.GetCRC()), 10) + " "
	str += strconv.FormatUint((rec.GetTimeStamp()), 10) + " "
	str += strconv.FormatInt(int64(rec.GetTombStone()), 10) + " "
	str += strconv.FormatUint((rec.GetKeySize()), 10) + " "
	str += strconv.FormatUint((rec.GetValueSize()), 10) + " "
	str += rec.GetKey() + " "
	for _, v := range rec.GetValue() {
		str += strconv.Itoa(int(v))
	}
	str += " "
	return str
}

// NEMA POTREBE ZA SEEKOM AKO CES CITATI REDOM JEDAN PO JEDAN OD POCETKA SAMO PUSTIS U PETLJU
// DA CITA DO KRAJA I POZIVAS U SVAKOJ ITERACIJI READRECORD
// PRILIKOM SVAKOG CITANJA SAM SE POMERA NA NAREDNI
func ReadRecord(file *os.File) (*Record, error) {
	bytes := make([]byte, CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE)
	// citanje zaglavlja -> od CRC-a do pocetka kljuca
	_, err := io.ReadAtLeast(file, bytes, CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE)
	if err != nil {
		//fmt.Println("Greska kod citanja Header-a")
		//log.Fatal(err)
		return nil, err
	}
	// konvertovanje velicine kljuca i velicine vrednosti u brojeve
	keySize := binary.BigEndian.Uint64(bytes[KEY_SIZE_START:VALUE_SIZE_START])
	valueSize := binary.BigEndian.Uint64(bytes[VALUE_SIZE_START:KEY_START])

	// citanje kljuca i vrednosti
	bytes = append(bytes, ReadKey(file, keySize)...)
	bytes = append(bytes, ReadValue(file, valueSize)...)

	// nad objektom mozes koristiti sve one get-ere
	rec := NewRecordByte(bytes)
	return rec, nil
}

// FUNKCIJE VRACAJU NIZ BAJTOVA
func ReadCRCBytes(file *os.File) []byte {
	bytes := make([]byte, CRC_SIZE)
	_, err := io.ReadAtLeast(file, bytes, CRC_SIZE)
	if err != nil {
		fmt.Println("Greska kod citanja CRC-a")
		log.Fatal(err)
	}

	return bytes
}
func ReadTimestampBytes(file *os.File) []byte {
	bytes := make([]byte, TIMESTAMP_SIZE)
	_, err := io.ReadAtLeast(file, bytes, TIMESTAMP_SIZE)
	if err != nil {
		fmt.Println("Greska kod citanja TimeStamp-a")
		log.Fatal(err)
	}
	return bytes
}
func ReadTombstoneBytes(file *os.File) []byte {
	bytes := make([]byte, TOMBSTONE_SIZE)
	_, err := io.ReadAtLeast(file, bytes, TOMBSTONE_SIZE)
	if err != nil {
		fmt.Println("Greska kod citanja TombStone-a")
		log.Fatal(err)
	}
	return bytes
}
func ReadKeySizeBytes(file *os.File) []byte {
	bytes := make([]byte, KEY_SIZE_SIZE)
	_, err := io.ReadAtLeast(file, bytes, KEY_SIZE_SIZE)
	if err != nil {
		fmt.Println("Greska kod citanja KeySize-a")
		log.Fatal(err)
	}
	return bytes
}
func ReadValueSizeBytes(file *os.File) []byte {
	bytes := make([]byte, VALUE_SIZE_SIZE)
	_, err := io.ReadAtLeast(file, bytes, VALUE_SIZE_SIZE)
	if err != nil {
		fmt.Println("Greska kod citanja ValueSize-a")
		log.Fatal(err)
	}
	return bytes
}
func ReadKeyBytes(file *os.File, keySize uint64) []byte {
	bytes := make([]byte, keySize)
	_, err := io.ReadAtLeast(file, bytes, int(keySize))
	if err != nil {
		fmt.Println("Greska kod citanja Key-a")
		log.Fatal(err)
	}
	return bytes
}

func ReadValueBytes(file *os.File, valueSize uint64) []byte {
	bytes := make([]byte, valueSize)
	_, err := io.ReadAtLeast(file, bytes, int(valueSize))
	if err != nil {
		fmt.Println("Greska kod citanja Value-a")
		log.Fatal(err)
	}

	return bytes
}

// POZICIONIRANJE NA ZADATU LOKACIJU JE OCEKIVANO
// FUNKCIJE VRACAJU BROJEVE/STRINGOVE KAO PODATKE
func ReadCRC(file *os.File) uint32 {
	bytes := make([]byte, CRC_SIZE)
	_, err := io.ReadAtLeast(file, bytes, CRC_SIZE)
	if err != nil {
		fmt.Println("Greska kod citanja CRC-a")
		log.Fatal(err)
	}

	return binary.BigEndian.Uint32(bytes)
}
func ReadTimestamp(file *os.File) uint64 {
	bytes := make([]byte, TIMESTAMP_SIZE)
	_, err := io.ReadAtLeast(file, bytes, TIMESTAMP_SIZE)
	if err != nil {
		fmt.Println("Greska kod citanja TimeStamp-a")
		log.Fatal(err)
	}
	return binary.BigEndian.Uint64(bytes)
}
func ReadTombstone(file *os.File) byte {
	bytes := make([]byte, TOMBSTONE_SIZE)
	_, err := io.ReadAtLeast(file, bytes, TOMBSTONE_SIZE)
	if err != nil {
		fmt.Println("Greska kod citanja TombStone-a")
		log.Fatal(err)
	}
	return bytes[0]
}
func ReadKeySize(file *os.File) uint64 {
	bytes := make([]byte, KEY_SIZE_SIZE)
	_, err := io.ReadAtLeast(file, bytes, KEY_SIZE_SIZE)
	if err != nil {
		fmt.Println("Greska kod citanja KeySize-a")
		log.Fatal(err)
	}
	return binary.BigEndian.Uint64(bytes)
}
func ReadValueSize(file *os.File) uint64 {
	bytes := make([]byte, VALUE_SIZE_SIZE)
	_, err := io.ReadAtLeast(file, bytes, VALUE_SIZE_SIZE)
	if err != nil {
		fmt.Println("Greska kod citanja ValueSize-a")
		log.Fatal(err)
	}
	return binary.BigEndian.Uint64(bytes)
}
func ReadKey(file *os.File, keySize uint64) string {
	bytes := make([]byte, keySize)
	_, err := io.ReadAtLeast(file, bytes, int(keySize))
	if err != nil {
		fmt.Println("Greska kod citanja Key-a")
		log.Fatal(err)
	}
	return string(bytes)
}

func ReadValue(file *os.File, valueSize uint64) []byte {
	bytes := make([]byte, valueSize)
	_, err := io.ReadAtLeast(file, bytes, int(valueSize))
	if err != nil {
		fmt.Println("Greska kod citanja Value-a")
		log.Fatal(err)
	}

	return bytes
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
