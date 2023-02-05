package application

import (
	record "NAiSP/Structures/Record"
	types "NAiSP/Structures/Types"
	"fmt"
)

func (app *App) AddNewCMS() {
	var epsilon float64
	for {
		e := app.ReadValue("Unesite vrednost epsilona kod za formiranje novog CMS: ")
		number, err := checkFloat(e)
		if !err {
			fmt.Println("Lose ste uneli vrednost epsilona. Probajte Ponovo.")
			continue
		}
		epsilon = number
		break
	}

	var delta float64
	for {
		d := app.ReadValue("Unesite vrednost delte: ")
		number, err := checkFloat(d)
		if !err {
			fmt.Println("Lose ste vrednost delte. Probajte Ponovo.")
			continue
		} else if number > 0.1 {
			fmt.Println("Velicina delte mora biti manja od 0.1")
			continue
		}
		delta = number
		break

	}

	var key string
	for {
		keyP := app.ReadValue("Unesite kljuc po kojim ce se cuvati: ")
		keyP = CMS + USER + keyP
		value := app.ReadPath.Read(keyP)

		if !check(key) {
			fmt.Println("Ne mozete koristiti ovaj kljuc.  Molim vas unesite novi kljuc.")
			continue
		} else if value != nil {
			fmt.Println("Vec postoji CMS pod ovakvim imenom. Molim vas unesite novi kljuc.")
			continue
		}
		key = keyP
		break

	}
	value := types.AddCMS(epsilon, delta)
	record := record.NewRecordKeyValue(key, value, 0)
	app.WritePath.Write(record)

}

func (app *App) DeleteCMS() {

	key := app.ReadValue("Unesite kljuc CMS-a kojeg zelite da obrisete: ")
	key = CMS + USER + key
	record := record.NewRecordKeyValue(key, []byte{0}, 1)
	app.WritePath.Write(record)

}

func (app *App) AddElementCMS() {

	key := app.ReadValue("Unesite kljuc CMS-a: ")
	key = CMS + USER + key
	value := app.ReadPath.Read(key)
	if value == nil {
		fmt.Println("Ne postoji ovaj CMS.")
		return
	}

	elemnt := app.ReadValue("Unesite kljuc elementa kog zeliteda dodate: ")

	cms := types.AppendElementCMS(elemnt, value)
	record := record.NewRecordKeyValue(key, cms, 0)
	app.WritePath.Write(record)
}

func (app *App) CheckElementCMS() {

	key := app.ReadValue("Unesite kljuc CMS-a: ")
	key = CMS + USER + key
	value := app.ReadPath.Read(key)
	if value == nil {
		fmt.Println("Lose ste uneli kljuc CMS-a ili ne postoji u bazi.")
		return
	}

	element := app.ReadValue("Unesite element cije ponavljanje zelite da proverite: ")
	numOfRepetiton := types.GetElementRepetitionsCMS(element, value)
	fmt.Println("Broj ponavaljanja je: ", numOfRepetiton)

}
