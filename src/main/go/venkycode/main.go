package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
)

type accumulator struct {
	name  [150]byte
	sum   int64
	count int64
	min   int64
	max   int64
}

var globalAccumulators = make(map[[150]byte]*accumulator)

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

	for chunkedAccumulator := range chunkedAccumulators {
		acc, ok := globalAccumulators[chunkedAccumulator.name]
		if !ok {
			globalAccumulators[chunkedAccumulator.name] = chunkedAccumulator
		} else {
			acc.sum += chunkedAccumulator.sum
			acc.count += chunkedAccumulator.count
			if chunkedAccumulator.min < acc.min {
				acc.min = chunkedAccumulator.min
			}
			if chunkedAccumulator.max > acc.max {
				acc.max = chunkedAccumulator.max
			}
		}
	}

	names := make([][150]byte, 0, len(globalAccumulators))
	for name := range globalAccumulators {
		names = append(names, name)
	}

	sort.Slice(names, func(i, j int) bool {
		return lesser(names[i], names[j])
	})

	outs := make([]string, 0, len(names))
	for _, name := range names {
		acc := globalAccumulators[name]
		min := float64(acc.min) / 10.0
		avg := (float64(acc.sum) / float64(acc.count)) / 10.0
		max := float64(acc.max) / 10.0
		out := (toString(name[:]) + "=" + strconv.FormatFloat(min, 'f', 1, 32) + "/" + strconv.FormatFloat(avg, 'f', 1, 32) + "/" + strconv.FormatFloat(max, 'f', 1, 32))
		outs = append(outs, out)
		println()
	}

	fmt.Printf("{%s}\n", strings.Join(outs, ", "))
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

// todo use SIMD
func lesser(a, b [150]byte) bool {
	// a and b are representations of strings
	// '%' is a terminator character
	for i := 0; i < 150; i++ {
		if a[i] == CUSTOM_TERMINATOR {
			return true
		}
		if b[i] == CUSTOM_TERMINATOR {
			return false
		}
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

func toString(b []byte) string {
	newBytes := make([]byte, 0, len(b))
	for _, c := range b {
		if c == CUSTOM_TERMINATOR {
			break
		}
		newBytes = append(newBytes, c)
	}
	return string(newBytes)
}
