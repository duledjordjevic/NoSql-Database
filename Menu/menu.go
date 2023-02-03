package menu

import (
	"fmt"
)

func PrintMenu() {
	fmt.Println()
	fmt.Println("-------------------- MENU --------------------")
	fmt.Println("1 - Dodovanje elementa(kljuc : vrednost)")
	fmt.Println("2 - Brisanje elementa")
	fmt.Println("3 - Pretraga")
	fmt.Println("4 - Uradi kompakciju fajlova u bazi")
	fmt.Println("5 - Ostale funkcionalnosti")
	fmt.Println("X - Izlazak iz programa")
}

func PrintSearchMenu() {
	fmt.Println()
	fmt.Println("-------------------- Vrste pretrage --------------------")
	fmt.Println("1 - Obicna pretraga")
	fmt.Println("2 - RANGE SCAN")
	fmt.Println("3 - LIST")
	fmt.Println("4 - Nazad")
	fmt.Println("X - Izlazak iz programa")
}

func PrintOtherMenu() {
	fmt.Println()
	fmt.Println("-------------------- Ostale funkcionalnosti --------------------")
	fmt.Println("1 - BloomFilter funkcionalnosti")
	fmt.Println("2 - CMS funkcionalnosti")
	fmt.Println("3 - HLL funkcionalnosti")
	fmt.Println("4 - SimHash funkcionalnosti")
	fmt.Println("5 - Nazad")
	fmt.Println("X - Izlazak iz programa")
}

func PrintBloomFilterMenu() {
	fmt.Println()
	fmt.Println("-------------------- BloomFilter funkcionalnosti --------------------")
	fmt.Println("1 - Dodavanje novog BloomFilter-a u bazu") //AddBloomFilter(expectedElements int, falsePositiveRate float64)
	fmt.Println("2 - Brisanje BloomFilter-a iz baze")
	fmt.Println("3 - Dodavanje elementa")
	fmt.Println("4 - Provera elementa")
	fmt.Println("5 - Nazad")
	fmt.Println("X - Izlazak iz programa")

}

func PrintCMSMenu() {
	fmt.Println()
	fmt.Println("-------------------- CMS funkcionalnosti --------------------")
	fmt.Println("1 - Dodavanje novog CMS-a u bazu") //add cms 0.1, 0.9
	fmt.Println("2 - Brisanje CMS-a iz baze")       // kljuc tb 1    //
	fmt.Println("3 - Dodavanje elementa")           // AppendElementCMS
	fmt.Println("4 - Provera ponavljanja elementa") // GetElementRepetitionsCMS
	fmt.Println("5 - Nazad")
	fmt.Println("X - Izlazak iz programa")
}

func PrintHLLMenu() {
	fmt.Println()
	fmt.Println("-------------------- HLL funkcionalnosti --------------------")
	fmt.Println("1 - Dodavanje novog HLL-a u bazu") // addhll
	fmt.Println("2 - Brisanje HLL-a iz baze")       // kljuc tb 1
	fmt.Println("3 - Dodavanje elementa")           // AppendElementHLL
	fmt.Println("4 - Provera kardinalnosti")        // CheckCardinalityHLL
	fmt.Println("5 - Nazad")
	fmt.Println("X - Izlazak iz programa")
}

func PrintSimHashMenu() {
	fmt.Println()
	fmt.Println("-------------------- SimHash funkcionalnosti --------------------")
	fmt.Println("1 - Dodavanje novog SimHash-a u bazu") // AddSimHash
	fmt.Println("2 - Brisanje SimHash-a iz baze")       // kljuc tb 1
	fmt.Println("3 - Provera sa drugim tekstom")        // CompareSimHash
	fmt.Println("4 - Nazad")
	fmt.Println("X - Izlazak iz programa")
}
