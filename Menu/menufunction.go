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
			if app.TokenBucket.GetPermission() {
				app.Put()
			}

		} else if input == "2" {
			if app.TokenBucket.GetPermission() {
				app.Delete()
			}

		} else if input == "3" {
			search(app)

		} else if input == "4" {
			if app.TokenBucket.GetPermission() {
				app.Compaction()
			}

		} else if input == "5" {

			other(app)

		} else if input == "X" || input == "x" {

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
			if app.TokenBucket.GetPermission() {
				app.Get()
			}

		} else if input == "2" {
			if app.TokenBucket.GetPermission() {
				app.RangeScan()
			}

		} else if input == "3" {
			if app.TokenBucket.GetPermission() {
				app.List()
			}

		} else if input == "4" {

			// Back
			return

		} else if input == "X" || input == "x" {

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

		} else if input == "X" || input == "x" {

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
			if app.TokenBucket.GetPermission() {
				app.AddBloom()
			}

		} else if input == "2" {

			// delete
			if app.TokenBucket.GetPermission() {
				app.DeleteBloom()
			}

		} else if input == "3" {

			// add element
			if app.TokenBucket.GetPermission() {
				app.AddElementBloom()
			}

		} else if input == "4" {

			// check
			if app.TokenBucket.GetPermission() {
				app.CheckElementBloom()
			}

		} else if input == "5" {

			// Back
			return

		} else if input == "X" || input == "x" {

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
			if app.TokenBucket.GetPermission() {
				app.AddNewCMS()
			}
		} else if input == "2" {
			if app.TokenBucket.GetPermission() {
				app.DeleteCMS()
			}
		} else if input == "3" {
			if app.TokenBucket.GetPermission() {
				app.AddElementCMS()
			}
		} else if input == "4" {
			if app.TokenBucket.GetPermission() {
				app.CheckElementCMS()
			}
		} else if input == "5" {

			// Back
			return

		} else if input == "X" || input == "x" {

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
			if app.TokenBucket.GetPermission() {
				app.AddHll()
			}

		} else if input == "2" {

			// delet
			if app.TokenBucket.GetPermission() {
				app.DeleteHLL()
			}

		} else if input == "3" {

			// add element
			if app.TokenBucket.GetPermission() {
				app.AddElementHLL()
			}

		} else if input == "4" {

			// chech
			if app.TokenBucket.GetPermission() {
				app.CheckCardHLL()
			}

		} else if input == "5" {

			// Back
			return

		} else if input == "X" || input == "x" {

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
			if app.TokenBucket.GetPermission() {
				app.AddSH()
			}

		} else if input == "2" {

			// delet
			if app.TokenBucket.GetPermission() {
				app.DeleteSH()
			}

		} else if input == "3" {

			// check
			if app.TokenBucket.GetPermission() {
				app.CheckSH()
			}

		} else if input == "4" {

			// Back
			return

		} else if input == "X" || input == "x" {

			// EXIT
			app.End()

		} else {
			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")
		}

	}
}
