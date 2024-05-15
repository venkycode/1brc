package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
)

const numRows int64 = 1000000000
const numBytesIn2GB int64 = 2 * 1024 * 1024 * 1024

type accumulator struct {
	name  string
	sum   float64
	count int
	min   float64
	max   float64
}

var accumulators = make(map[string]*accumulator)

func main() {
	input := flag.String("input", "", "Input file")
	output := flag.String("output", "", "Output file")
	flag.Parse()

	inputFile, err := os.Open(*input)
	panicOnError(err)
	defer inputFile.Close()

	var readTill int64 = 0
	var name string
	var temperature float64
	buffer := make([]byte, numBytesIn2GB)
	bufferPtr := int64(0)
	for i := int64(0); i < numRows; i++ {
		name, temperature, readTill = parseLine(inputFile, readTill, buffer, &bufferPtr)
		if readTill == 0 {
			panic("could not read till new line")
		}
		_, err := inputFile.Seek(readTill, 0)
		panicOnError(err)

		acc, ok := accumulators[name]
		if !ok {
			acc = &accumulator{name: name, sum: temperature, count: 1, min: temperature, max: temperature}
			accumulators[name] = acc
		} else {
			acc.sum += temperature
			acc.count++
			if temperature < acc.min {
				acc.min = temperature
			}
			if temperature > acc.max {
				acc.max = temperature
			}
		}

		if i%1000000 == 0 {
			fmt.Printf("Processed %d rows\n", i)
		}
	}

	outputFile, err := os.Create(*output)
	panicOnError(err)
	defer outputFile.Close()

	names := make([]string, 0, len(accumulators))
	for name := range accumulators {
		names = append(names, name)
	}

	sort.Slice(names, func(i, j int) bool {
		return names[i] < names[j]
	})

	for _, name := range names {
		acc := accumulators[name]
		_, err := outputFile.WriteString(name + ";" + strconv.FormatFloat(acc.sum/float64(acc.count), 'f', 1, 32) + ";" + strconv.FormatFloat(acc.min, 'f', 1, 32) + ";" + strconv.FormatFloat(acc.max, 'f', 1, 32) + "\n")
		panicOnError(err)
	}

}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func parseLine(file *os.File, offset int64, localBuffer []byte, localBufferPtr *int64) (name string, temperature float64, readTill int64) {
	newLineAt := int64(-1)
	semicolonAt := int64(-1)
	for i := *localBufferPtr; i < numBytesIn2GB; i++ {
		if semicolonAt == -1 && localBuffer[i] == ';' {
			semicolonAt = i
		}
		if localBuffer[i] == '\n' {
			newLineAt = i
			break
		}
	}

	if newLineAt == -1 || semicolonAt == -1 {
		_, err := file.ReadAt(localBuffer, offset)
		if err != nil && err != io.EOF {
			panicOnError(err)
		}
		*localBufferPtr = 0
		return parseLine(file, offset, localBuffer, localBufferPtr)
	}

	nameB, temperatureB := localBuffer[*localBufferPtr:semicolonAt], localBuffer[semicolonAt+1:newLineAt]

	name = string(nameB)

	temperature, err := strconv.ParseFloat(string(temperatureB), 32) //TODO: parse manually
	panicOnError(err)

	readTill = offset + int64(newLineAt) + 1
	*localBufferPtr = newLineAt + 1

	return
}
