package cos418_hw1_1

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strings"
)

// Find the top K most common words in a text document.
//
//	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
//
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuations and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	// TODO: implement me
	// HINT: You may find the `strings.Fields` and `strings.ToLower` functions helpful
	// HINT: To keep only alphanumeric characters, use the regex "[^0-9a-zA-Z]+"

	dat, err := ioutil.ReadFile(path)
	if err != nil {
		return nil
	}
	low_string := strings.ToLower((string(dat)))
	reg := regexp.MustCompile(`[^0-9a-zA-Z]+`)
	words := strings.Fields(low_string)
	cnt := make(map[string]int)
	for _, word := range words {
		if len(word) >= charThreshold {
			clean_word := reg.ReplaceAllString(word, "${1}")
			cnt[clean_word]++
		}
	}
	counts := make([]WordCount, 0)
	for k, v := range cnt {
		x := WordCount{k, v}
		counts = append(counts, x)
	}
	sortWordCounts(counts)
	return counts[:numWords]
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
