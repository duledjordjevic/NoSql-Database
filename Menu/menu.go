package menu

import "fmt"

func PrintMenu() {
	fmt.Println()
	fmt.Println("-------------------- MENU --------------------")
	fmt.Println("1. Dodovanje elementa(kljuc : vrednost)")
	fmt.Println("2. Brisanje elementa")
	fmt.Println("3. Pretraga")
	fmt.Println("4. Ostale funkcionalnosti")
	fmt.Println("5. Izlazak iz programa")
}

func PrintSearchMenu() {
	fmt.Println()
	fmt.Println("-------------------- Vrste pretrage --------------------")
	fmt.Println("1. Obicna pretraga")
	fmt.Println("2. RANGE SCAN")
	fmt.Println("3. LIST")
	fmt.Println("4. Nazad")
}

func PrintOtherMenu() {
	fmt.Println()
	fmt.Println("-------------------- Ostale funkcionalnosti --------------------")
	fmt.Println("1. BloomFilter funkcionalnosti")
	fmt.Println("2. CMS funkcionalnosti")
	fmt.Println("3. HLL funkcionalnosti")
	fmt.Println("4. SimHash funkcionalnosti")
	fmt.Println("5. Nazad")
}

func PrintBloomFilterMenu() {
	fmt.Println()
	fmt.Println("-------------------- BloomFilter funkcionalnosti --------------------")
	fmt.Println("1. Dodavanje novog BloomFilter-a u bazu")
	fmt.Println("2. Briasnje BloomFilter-a iz baze")
	fmt.Println("3. Dodavanje elementa")
	fmt.Println("4. Provera elementa")
	fmt.Println("5. Nazad")

}

func PrintCMSMenu() {
	fmt.Println()
	fmt.Println("-------------------- CMS funkcionalnosti --------------------")
	fmt.Println("1. Dodavanje novog CMS-a u bazu")
	fmt.Println("2. Briasnje CMS-a iz baze")
	fmt.Println("3. Dodavanje elementa")
	fmt.Println("4. Provera elementa")
	fmt.Println("5. Nazad")
}

func PrintHLLMenu() {
	fmt.Println()
	fmt.Println("-------------------- HLL funkcionalnosti --------------------")
	fmt.Println("1. Dodavanje novog HLL-a u bazu")
	fmt.Println("2. Briasnje HLL-a iz baze")
	fmt.Println("3. Dodavanje elementa")
	fmt.Println("4. Provera kardinalnosti")
	fmt.Println("5. Nazad")
}

func PrintSimHashMenu() {
	fmt.Println()
	fmt.Println("-------------------- SimHash funkcionalnosti --------------------")
	fmt.Println("1. Dodavanje novog SimHash-a u bazu")
	fmt.Println("2. Briasnje SimHash-a iz baze")
	fmt.Println("3. Provera sa drugom")
	fmt.Println("4. Nazad")
}
