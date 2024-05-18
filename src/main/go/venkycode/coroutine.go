package main

import (
	"os"
	"runtime"
	"sync"
)

const (
	AccumulatorChannelBufferSize = 1000
)

func processChunk(
	fileName string,
	i, j int64,
) map[string]*accumulator {
	file, err := os.Open(fileName)
	panicOnError(err)
	defer file.Close()

	bufferSize := (j - i)
	buffer := make([]byte, bufferSize)
	_, err = file.ReadAt(buffer, i)
	panicOnError(err)
	bufferPtr := int64(0)
	i, bufferPtr = skipDirtyLine(file, i, buffer, bufferPtr)
	var name string
	var temperature float64
	var accumulators = make(map[string]*accumulator)
	for i < j {
		name, temperature, i, bufferPtr = parseLine(file, i, buffer, bufferPtr)

		acc, ok := accumulators[name]
		if !ok {
			acc = &accumulator{
				name:  name,
				sum:   temperature,
				count: 1,
				min:   temperature,
				max:   temperature,
			}
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
	}

	return accumulators
}

func processFile(fileName string) <-chan *accumulator {
	file, err := os.Open(fileName)
	panicOnError(err)
	defer file.Close()

	info, err := file.Stat()
	panicOnError(err)

	numGoroutines := runtime.NumCPU() * 3
	numChunks := numGoroutines * 20

	chunkSize := info.Size() / int64(numChunks)

	output := make(chan *accumulator, AccumulatorChannelBufferSize)

	go func() {
		wg := &sync.WaitGroup{}
		// sema := semaphore.NewWeighted(numGoroutines)
		sema := newSemaphore(int(numGoroutines))
		for i := int64(0); i < int64(numChunks); i++ {
			chunckStart := i * chunkSize
			chunckEnd := min((i+1)*chunkSize, info.Size())
			wg.Add(1)
			sema.acquire()
			go func() {
				defer wg.Done()
				defer sema.release()
				accumulators := processChunk(fileName, chunckStart, chunckEnd)
				for _, acc := range accumulators {
					output <- acc
				}
			}()
		}
		go func() {
			wg.Wait()
			close(output)
		}()

	}()

	return output
}

type Semaphore struct {
	sem chan struct{}
}

func newSemaphore(n int) *Semaphore {
	return &Semaphore{sem: make(chan struct{}, n)}
}

func (s *Semaphore) acquire() {
	s.sem <- struct{}{}
}

func (s *Semaphore) release() {
	<-s.sem
}
