package main

import (
	"os"
	"runtime"
	"sync"

	"github.com/venkycode/1brc/models"
	"github.com/venkycode/1brc/parser"
	"github.com/venkycode/1brc/trie"
)

const (
	AccumulatorChannelBufferSize = 1000
)

func processChunk(
	fileName string,
	i, j int64,
) *trie.Node {
	file, err := os.Open(fileName)
	panicOnError(err)
	defer file.Close()

	bufferSize := (j - i)
	buffer := make([]byte, bufferSize)
	_, err = file.ReadAt(buffer, i)
	panicOnError(err)
	bufferPtr := int64(0)
	i, bufferPtr = parser.SkipDirtyLine(file, i, buffer, bufferPtr)
	var name [150]byte
	var temperature int64
	accumulators := trie.NewTrie()
	for i < j {
		name, temperature, i, bufferPtr = parser.ParseLine(file, i, buffer, bufferPtr)

		accumulators.Insert(models.NewAccumulator(name, temperature))
	}

	return accumulators
}

func processFile(fileName string) <-chan *models.Accumulator {
	file, err := os.Open(fileName)
	panicOnError(err)
	defer file.Close()

	info, err := file.Stat()
	panicOnError(err)

	numGoroutines := runtime.NumCPU() * 3
	numChunks := numGoroutines * 20

	chunkSize := info.Size() / int64(numChunks)

	output := make(chan *models.Accumulator, AccumulatorChannelBufferSize)

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
				accumulators.Walk(output)
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
