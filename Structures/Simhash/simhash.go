package simhash

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

func (sh *SimHash) GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func (sh *SimHash) ToBinary(s string) string {
	res := ""
	for _, c := range s {
		res = fmt.Sprintf("%s%.8b", res, c)
	}
	return res
}

type SimHash struct {
	WordMap *map[string]int
	Value   string
}

func CreateSimHash(value string) *SimHash {
	sh := SimHash{}
	listWord := sh.Preaper(value)
	wordMap := sh.WeightValue(listWord)
	sh.WordMap = &wordMap
	sh.Value = sh.Hasing(*sh.WordMap)
	return &sh
}

// preapers the text for further processing
func (sh *SimHash) Preaper(value string) []string {
	// remove all symbols, except letters
	re := regexp.MustCompile("[^a-zA-Z0-9 ]+")
	value = re.ReplaceAllString(value, " ")
	words := strings.Fields(value)
	var newWords []string

	for _, word := range words {
		// remove word, if word is stop word
		word := strings.ToLower(word)
		if !sh.CheckStopWord(word) {
			newWords = append(newWords, word)
		}
	}

	return newWords
}

// check that word is stop word
func (sh *SimHash) CheckStopWord(value string) bool {
	if v, ok := english[value]; ok {
		return v
	}
	return false
}

// Give a weight value to each word
func (sh *SimHash) WeightValue(value []string) map[string]int {
	wordMap := make(map[string]int)
	for _, word := range value {
		word := strings.ToLower(word)
		if v, ok := wordMap[word]; ok {
			wordMap[word] += v
		} else {
			wordMap[word] = 1
		}
	}
	return wordMap
}

func (sh *SimHash) Hasing(wordMap map[string]int) string {
	wordsBinaryMap := make(map[string]int)
	for keySting, value := range wordMap {
		wordsBinaryMap[sh.ToBinary(sh.GetMD5Hash(keySting))] = value
	}
	var value string
	var solution int64
	for position := range sh.ToBinary(sh.GetMD5Hash("1")) {
		solution = 0
		for keyBinaryString, valueBinaryString := range wordsBinaryMap {
			if keyBinaryString[position] == 49 {
				solution += int64(valueBinaryString)
			} else {
				solution += (-1) * int64(valueBinaryString)
			}
		}
		if solution < 0 {
			value += "0"
		} else {
			value += "1"
		}
	}
	return value
}

func (sh *SimHash) Comparation(value string) string {
	value = sh.Hasing(sh.WeightValue(sh.Preaper(value)))

	var resultStr string
	for position := range value {
		if value[position] == sh.Value[position] {
			resultStr += "0"
		} else {
			resultStr += "1"
		}
	}

	return resultStr
}
