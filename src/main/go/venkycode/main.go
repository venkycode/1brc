package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"

	"github.com/venkycode/1brc/models"
	"github.com/venkycode/1brc/parser"
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

	globalAccumulators := make(map[[150]byte]models.Accumulator)
	for chunkedAccumulator := range chunkedAccumulators {
		if existing, ok := globalAccumulators[chunkedAccumulator.Name]; ok {
			globalAccumulators[chunkedAccumulator.Name] = existing.Merge(chunkedAccumulator)
		} else {
			globalAccumulators[chunkedAccumulator.Name] = chunkedAccumulator
		}
	}

	results := []string{}

	globalAccumulatorsList := make([]models.Accumulator, 0, len(globalAccumulators))
	for _, acc := range globalAccumulators {
		globalAccumulatorsList = append(globalAccumulatorsList, acc)
	}
	sort.Slice(globalAccumulatorsList, func(i, j int) bool {
		for k := 0; k < len(globalAccumulatorsList[i].Name); k++ {
			if globalAccumulatorsList[i].Name[k] == parser.CUSTOM_TERMINATOR {
				return true
			}
			if globalAccumulatorsList[j].Name[k] == parser.CUSTOM_TERMINATOR {
				return false
			}
			if globalAccumulatorsList[i].Name[k] < globalAccumulatorsList[j].Name[k] {
				return true
			}
			if globalAccumulatorsList[i].Name[k] > globalAccumulatorsList[j].Name[k] {
				return false
			}
		}
		return true

	})

	for _, acc := range globalAccumulatorsList {
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
