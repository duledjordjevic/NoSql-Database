// Token Bucket algorithm

package tokenbucket

import (
	"fmt"
	"time"
)

// Capacity and Duration will get from config file
type TokenBucket struct {
	Capacity   int
	TokensLeft int
	LastReset  time.Time
	Duration   time.Duration
}

// Constructor
func NewTokenBucket() *TokenBucket {

	// for testing
	// return &TokenBucket{
	// 	Duration: 3000000000,
	// 	Capacity: 3,
	// }
	return &TokenBucket{}

}

// Get permission
func (tb *TokenBucket) GetPermission() bool {

	// Set time for current Token as current
	currentTime := time.Now()
	elapsedTime := currentTime.Sub(tb.LastReset)

	// Check if token is in range
	if elapsedTime <= tb.Duration {

		// Check if bucket is full
		if tb.TokensLeft > 0 {
			tb.TokensLeft -= 1
			return true
		}

		// too many accesses, false message
		fmt.Println("Zahtev nije prihvacen, prevelik broj pristupa u jedinici vremena.")
		return false
	}

	// Make new bucket
	tb.LastReset = currentTime
	tb.TokensLeft = tb.Capacity - 1
	return true
}
