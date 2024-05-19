package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"

	"github.com/venkycode/1brc/models"
	"github.com/venkycode/1brc/parser"
	"github.com/venkycode/1brc/trie"
)

func main() {
	profile := flag.Bool("profile", false, "Profile file")
	input := flag.String("input", "", "Input file")
	flag.Parse()

	if *profile {
		f, err := os.Create("profile.out")
		panicOnError(err)
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()
	}

	inputFile, err := os.Open(*input)
	panicOnError(err)
	defer inputFile.Close()

	chunkedAccumulators := processFile(*input)

	globalAccumulator := trie.NewFlatTrie()

	for chunkedAccumulator := range chunkedAccumulators {
		globalAccumulator.Insert(chunkedAccumulator)
	}

	orderedOutput := make(chan *models.Accumulator, 1024)

	go func() {
		globalAccumulator.WalkInOrder(orderedOutput)
		close(orderedOutput)
	}()

	results := []string{}

	for acc := range orderedOutput {
		min := float64(acc.Min) / 10.0
		avg := (float64(acc.Sum) / float64(acc.Count)) / 10.0
		max := float64(acc.Max) / 10.0
		name := acc.Name
		result := (parser.ToString(name[:]) + "=" + strconv.FormatFloat(min, 'f', 1, 32) + "/" + strconv.FormatFloat(avg, 'f', 1, 32) + "/" + strconv.FormatFloat(max, 'f', 1, 32))
		results = append(results, result)
		println()
	}

	fmt.Printf("{%s}\n", strings.Join(results, ", "))
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
