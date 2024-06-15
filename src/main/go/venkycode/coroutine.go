package main

import (
	"os"
	"runtime"
	"sync"

	"github.com/venkycode/1brc/models"
	"github.com/venkycode/1brc/parser"
)

const (
	AccumulatorChannelBufferSize = 1000000
)

func processChunk(
	fileName string,
	i, j int64,
) map[[150]byte]models.Accumulator {
	file, err := os.Open(fileName)
	panicOnError(err)
	defer file.Close()

	bufferSize := (j - i)
	buffer := make([]byte, bufferSize)
	_, err = file.ReadAt(buffer, i)
	panicOnError(err)
	bufferPtr := int64(0)
	i, bufferPtr = parser.SkipDirtyLine(file, i, buffer, bufferPtr)
	out := make(map[[150]byte]models.Accumulator)
	for i < j {
		var name [150]byte
		var temperature int64
		name, temperature, i, bufferPtr = parser.ParseLine(file, i, buffer, bufferPtr)
		if existing, ok := out[name]; ok {
			out[name] = existing.Merge(models.NewWithoutName(temperature))
		} else {
			out[name] = models.New(name, temperature)
		}
	}

	return out
}

func processFile(fileName string) <-chan models.Accumulator {
	file, err := os.Open(fileName)
	panicOnError(err)
	defer file.Close()

	info, err := file.Stat()
	panicOnError(err)

	numGoroutines := runtime.NumCPU() * 3
	numChunks := numGoroutines * 20

	chunkSize := info.Size() / int64(numChunks)

	output := make(chan models.Accumulator, AccumulatorChannelBufferSize)

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
