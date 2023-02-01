package readpath

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	lru "NAiSP/Structures/LRUcache"
	memtable "NAiSP/Structures/Memtable"
	sstable "NAiSP/Structures/Sstable"
	"fmt"
	"io/ioutil"
	"log"
)

type ReadPath struct {
	MemTable    *memtable.MemTable
	Lru         *lru.LRUCache
	BloomFilter *bloomfilter.BloomFilter
	// Need SsTable
}

func (rp *ReadPath) Read(key string) []byte {

	// First check in MemTable
	record := rp.MemTable.Find(key)
	if record != nil {
		rp.Lru.AddElement(record.GetKey(), record.GetValue())
		return record.GetValue()
	}

	// Next check in Cache
	value := rp.Lru.GetElement(key)
	if value != nil {
		return value
	}

	// Next check Bloom Filter
	rp.BloomFilter.Encode("./Data/GlobalFilter/bloomfilter.gob")
	found := rp.BloomFilter.Find(key)
	if !found {
		// If not in bloom we can be sure it's not there
		return nil
	}

	// Opening directory that contains data files
	folders, err := ioutil.ReadDir("./Data/TOC/")
	if err != nil {
		fmt.Println("Greska kod citanja direktorijuma: ", err)
		log.Fatal(err)
	}

	// find valid summary
	for _, folder := range folders {
		files, err := ioutil.ReadDir("./Data/TOC/" + folder.Name())
		if err != nil {
			fmt.Println("Greska kod citanja direktorijuma: ", err)
			log.Fatal(err)
		}
		for i := len(files) - 1; i >= 0; i-- {
			Sstable := sstable.NewSStableFromTOC("./Data/TOC/" + folder.Name() + "/" + files[i].Name())
			if Sstable == nil {
				fmt.Println("Error: Lose ucitan TOC: " + files[i].Name())
			}
			record = Sstable.Search(key)
			if record != nil {
				return record.GetValue()
			}

		}
	}
	return nil
}
