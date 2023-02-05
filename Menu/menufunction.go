package menu

import (
	application "NAiSP/App"
	"fmt"
)

// Global menu
func Start(app *application.App) {
	for {
		PrintMenu()
		input := app.ReadValue("Unesite komandu: ")
		if input == "1" {

			app.Put()

		} else if input == "2" {

			app.Delete()

		} else if input == "3" {

			search(app)

		} else if input == "4" {

			app.Compaction()

		} else if input == "5" {

			other(app)

		} else if input == "X" {

			app.End()
		} else {
			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")
		}

	}
}

// Search operation
func search(app *application.App) {
	for {
		PrintSearchMenu()
		input := app.ReadValue("Unesite komandu: ")
		if input == "1" {

			//Normal search
			app.Get()

		} else if input == "2" {

			app.RangeScan()

		} else if input == "3" {

			app.List()

		} else if input == "4" {

			// Back
			return

		} else if input == "X" {

			// EXIT
			app.End()

		} else {
			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")
		}

	}
}

// Other operation
func other(app *application.App) {
	for {
		PrintOtherMenu()
		input := app.ReadValue("Unesite komandu: ")
		if input == "1" {

			// Bloomfilter function
			bfMenu(app)

		} else if input == "2" {

			// CMS function
			cmsMenu(app)

		} else if input == "3" {

			// HLL function
			hllMenu(app)

		} else if input == "4" {

			// Sim Hash function
			shMenu(app)

		} else if input == "5" {

			// Back
			return

		} else if input == "X" {

			// EXIT
			app.End()

		} else {
			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")
		}

	}
}

// BLOOMFILTER operation menu
func bfMenu(app *application.App) {
	for {
		PrintBloomFilterMenu()
		input := app.ReadValue("Unesite komandu: ")
		if input == "1" {

			// ADD Bloomfilter function
			app.AddBloom()

		} else if input == "2" {

			// delete
			app.DeleteBloom()

		} else if input == "3" {

			// add element
			app.AddElementBloom()

		} else if input == "4" {

			// check
			app.CheckElementBloom()

		} else if input == "5" {

			// Back
			return

		} else if input == "X" {

			// EXIT
			app.End()

		} else {
			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")
		}

	}
}

// CMS operation menu
func cmsMenu(app *application.App) {
	for {
		PrintCMSMenu()
		input := app.ReadValue("Unesite komandu: ")
		if input == "1" {
			// add
		} else if input == "2" {
			// delet
		} else if input == "3" {
			// add element
		} else if input == "4" {
			// check
		} else if input == "5" {

			// Back
			return

		} else if input == "X" {

			// EXIT
			app.End()

		} else {
			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")
		}

	}
}

// HLL operation menu
func hllMenu(app *application.App) {
	for {
		PrintHLLMenu()
		input := app.ReadValue("Unesite komandu: ")
		if input == "1" {

			// add
			app.AddHll()

		} else if input == "2" {

			// delet
			app.DeleteHLL()

		} else if input == "3" {

			// add element
			app.AddElementHLL()

		} else if input == "4" {

			// chech
			app.CheckCardHLL()

		} else if input == "5" {

			// Back
			return

		} else if input == "X" {

			// EXIT
			app.End()

		} else {
			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")
		}

	}
}

// SIMHASH operation menu
func shMenu(app *application.App) {
	for {
		PrintSimHashMenu()
		input := app.ReadValue("Unesite komandu: ")
		if input == "1" {

			// Add
			app.AddSH()

		} else if input == "2" {

			// delet
			app.DeleteSH()

		} else if input == "3" {

			// check
			app.CheckSH()

		} else if input == "4" {

			// Back
			return

		} else if input == "X" {

			// EXIT
			app.End()

		} else {
			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")
		}

	}
}
