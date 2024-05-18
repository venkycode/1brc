package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

type accumulator struct {
	name  string
	sum   float64
	count int
	min   float64
	max   float64
}

var globalAccumulators = make(map[string]*accumulator)

func main() {
	input := flag.String("input", "", "Input file")
	flag.Parse()

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

	names := make([]string, 0, len(globalAccumulators))
	for name := range globalAccumulators {
		names = append(names, name)
	}

	sort.Slice(names, func(i, j int) bool {
		return names[i] < names[j]
	})

	outs := make([]string, 0, len(names))
	for _, name := range names {
		acc := globalAccumulators[name]
		out := (name + "=" + strconv.FormatFloat(acc.min, 'f', 1, 32) + "/" + strconv.FormatFloat(acc.sum/float64(acc.count), 'f', 1, 32) + "/" + strconv.FormatFloat(acc.max, 'f', 1, 32))
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

// todo pass struct to functions
func parseLine(file *os.File, offset int64, buffer []byte, bufferPtr int64) (name string, temperature float64, newOffset int64, newBufferPtr int64) {
	semicolonAt := int64(-1)
	newBufferPtr = bufferPtr
	newOffset = offset
	for newBufferPtr < int64(len(buffer)) {
		if buffer[newBufferPtr] == ';' {
			semicolonAt = newBufferPtr
		}
		if buffer[newBufferPtr] == '\n' {
			break
		}
		newBufferPtr++
		newOffset++
	}

	if newBufferPtr == int64(len(buffer)) || semicolonAt == -1 { // line is not complete
		name, temperature, newOffset = readOneLineFrom(file, offset)
		return
	}

	nameB, temperatureB := buffer[bufferPtr:semicolonAt], buffer[semicolonAt+1:newBufferPtr]
	newBufferPtr++ // skip '\n'
	newOffset++

	name = string(nameB)

	temperature, err := strconv.ParseFloat(string(temperatureB), 32) //TODO: parse manually
	panicOnError(err)

	return name, temperature, newOffset, newBufferPtr
}

func readOneLineFrom(file *os.File, offset int64) (name string, temperature float64, newOffset int64) {
	buffer := make([]byte, 400)
	_, err := file.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		panicOnError(err)
	}

	name, temperature, newOffset, _ = parseLine(file, offset, buffer, 0)
	return name, temperature, newOffset
}

func skipDirtyLine(file *os.File, offset int64, buffer []byte, bufferPtr int64) (newOffset int64, newBufferPtr int64) {

	if offset == 0 {
		return offset, bufferPtr
	}

	oneByte := make([]byte, 1)
	_, err := file.ReadAt(oneByte, offset-1)
	panicOnError(err)
	if oneByte[0] == '\n' {
		return offset, bufferPtr
	}

	newLineAt := int64(-1)
	for i := bufferPtr; i < int64(len(buffer)); i++ {
		if buffer[i] == '\n' {
			newLineAt = i
			break
		}
	}

	if newLineAt == -1 {
		return offset + int64(len(buffer)) - bufferPtr, int64(len(buffer))
	}

	return offset + newLineAt + 1 - bufferPtr, newLineAt + 1
}
