package merkle

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strings"
)

const (
	OUTFILE = "./Metadata.txt"
)

// directory -> putanja do diska/direktorijuma koji sadrzi zeljene datoteke
// Height -> rastojanje korena i najdaljeg lista
// Leaves -> broj ulaznih datoteka
// Nodes -> cvorovi merkle stabla

type MerkleTree struct {
	// uz ovaj directory bi trebalo da se doda i ulazni file ako se radi sa njim tj directory bi predstavljao to
	Source string
	Height int
	Leaves int
	Nodes  []string
}

// mozda nije neophodan kosntruktor
func NewMerkleTreeFile(filename string) *MerkleTree {
	return &MerkleTree{Source: filename, Leaves: 0}
}

func NewMerkleTreeDirectory(directory string) *MerkleTree {
	// kosntruktor klase MerkleTree
	// param: directory - direktorijum u kom se nalaze fajlovi za hesiranje(listovi stabla)
	// return: referenca stabla
	return &MerkleTree{Source: directory}
}

// DODATI FUNKCIJU KOJA CE PRAVITI OD JEDNOG SSTABLE-A MERKLE STABLO
// ZNACI OTVARA FILE BINARNE SSTABLE DATOTEKE REDOSLEDNO JE CITA I SVAKI SLOG DODAJE U NULTI NIVO MERKLE STABLA
// OSTATAK LOGIKE JE IDENTICAN

func (mt *MerkleTree) AddLeaf(data []byte) {
	mt.Leaves++
	hash := Hash(data[:])
	mt.Nodes = append(mt.Nodes, String(hash[:]))
}

func (mt *MerkleTree) GenerateLevelZeroDirectory() {
	// funckija koja pristupa zadatom direktorijumu, cita svaki fajl i hash-uje njegov sadrzaj
	// generise se listovi stabla

	// citanje direktorijuma
	files, err := ioutil.ReadDir(mt.Source)
	if err != nil {
		fmt.Println("Greska kod citanja direktorijuma: ", err)
		log.Fatal(err)
	}

	// racunanje visine stabla i broja listova
	mt.Leaves = len(files)

	// citanje sadrzaja fajlova
	for _, file := range files {
		// ovde se desava radnja

		//fmt.Println(file.Name())

		f, err := os.Open(mt.Source + "/" + file.Name())
		if err != nil {
			fmt.Println("Greska kod otvaranja fajla: ", err)
			log.Fatal(err)
		}

		data, err := ioutil.ReadAll(f)
		if err != nil {
			fmt.Println("Greska kod citanja sadrzaja fajla: ", err)
			log.Fatal(err)
		}
		// hash-iranje sadrzaja i dodavanje lista u stablo
		// proveri redosled da li je logicnije da bude String u Hash-u ili obrnuto
		// mt.Nodes = append(mt.Nodes, String(Hash(data)))
		hash := Hash(data[:])
		mt.Nodes = append(mt.Nodes, String(hash[:]))
		f.Close()

	}
}

func (mt *MerkleTree) LevelCap(level int) int {
	// level je visina na kojoj se nalaze cvorovi
	return int(math.Ceil(float64(mt.Leaves) / math.Pow(2, float64(mt.Height-level))))
}

func (mt *MerkleTree) GenerateMerkleTree() {
	// racunanje visine stabla
	mt.Height = int(math.Ceil(math.Log2(float64(mt.Leaves))))

	var child string
	var rest int
	counter := 0

	fmt.Println("Visina: ", mt.Height)

	for h := mt.Height - 1; h >= 0; h-- {
		//	krece se od maksimalne visine -> odnsono od listova ka korenu

		// broj elemenata stabla do pocetka generisanja novog nivoa
		fmt.Println("-----------------")
		fmt.Println("Nivo: ", h)

		// velicina prethodnog nivoa
		currentSize := len(mt.Nodes)
		// od kog elementa krecu deca novog nivoa
		offset := currentSize - mt.LevelCap(h+1)

		fmt.Println("CurrentSize: ", currentSize)
		fmt.Println("offset: ", offset)
		fmt.Println("-----------------")

		// u zavisnosti od offseta proveravamo parnost narednog nivoa
		if offset%2 == 0 {
			rest = 0
		} else {
			rest = 1
		}

		for i := offset; i < currentSize; i++ {
			fmt.Println("Brojac: ", counter)

			if i%2 == rest {
				child = mt.Nodes[i]
			} else {
				data := child + mt.Nodes[i]

				hash := Hash([]byte(data))
				mt.Nodes = append(mt.Nodes, String(hash[:]))

				fmt.Println("Normalno spajanje")
				child = ""
			}

			counter++
		}
		// ukoliko je neparan broj cvorova na trenutnom nivou -> dodavanje 'empty' hash-a
		if child != "" {
			hash := Hash([]byte(child))
			mt.Nodes = append(mt.Nodes, String(hash[:]))

			fmt.Println("Spajanje neparnog")
		}
	}
	fmt.Println("Broj blokova: ", len(mt.Nodes))

}

func (mt *MerkleTree) Encode() {
	file, err := os.OpenFile(mt.Source, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	for _, node := range mt.Nodes {
		_, err = file.WriteString(node + " ")
		if err != nil {
			panic(err)
		}
	}
}

func (mt *MerkleTree) Decode(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()

	// dodati koliko se jos 2 stvari u ovaj zapis -> visinu i broj cvorova na nultom nivou
	mt.Nodes = strings.Fields(scanner.Text())
}

func String(data []byte) string {
	return hex.EncodeToString(data[:])
}

func Hash(data []byte) [20]byte {
	// funkcija koja hash-uje sadrzaj fajla
	return sha1.Sum(data)
}
