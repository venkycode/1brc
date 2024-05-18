package main

import (
	"io"
	"os"
)

const (
	SEMICOLON         byte = ';'
	NEWLINE           byte = '\n'
	CUSTOM_TERMINATOR byte = '%'
)

// todo pass struct to functions
func parseLine(file *os.File, offset int64, buffer []byte, bufferPtr int64) (name [150]byte, temperature int64, newOffset int64, newBufferPtr int64) {
	semicolonAt := int64(-1)
	newBufferPtr = bufferPtr
	newOffset = offset
	for newBufferPtr < int64(len(buffer)) {
		if buffer[newBufferPtr] == SEMICOLON {
			semicolonAt = newBufferPtr
		}
		if buffer[newBufferPtr] == NEWLINE {
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

	copy(name[:], nameB)
	name[len(nameB)] = CUSTOM_TERMINATOR

	temperature = temperatureBToInt(temperatureB)

	return name, temperature, newOffset, newBufferPtr
}

const (
	DOT   byte = '.'
	ZERO  byte = '0'
	MINUS byte = '-'
)

// temperature will be in the form of "xx.x" i.e. only 1 decimal place
func temperatureBToInt(temperatureB []byte) int64 {
	temperature := int64(0)
	minus := false
	for i := 0; i < len(temperatureB); i++ {
		if temperatureB[i] == DOT {
			continue
		}
		if !minus && temperatureB[i] == MINUS {
			minus = true
			continue
		}
		temperature = temperature*10 + int64(temperatureB[i]-ZERO)
	}
	if minus {
		temperature = -temperature
	}
	return temperature

}

func readOneLineFrom(file *os.File, offset int64) (name [150]byte, temperature int64, newOffset int64) {
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
	if oneByte[0] == NEWLINE {
		return offset, bufferPtr
	}

	newLineAt := int64(-1)
	for i := bufferPtr; i < int64(len(buffer)); i++ {
		if buffer[i] == NEWLINE {
			newLineAt = i
			break
		}
	}

	if newLineAt == -1 {
		return offset + int64(len(buffer)) - bufferPtr, int64(len(buffer))
	}

	return offset + newLineAt + 1 - bufferPtr, newLineAt + 1
}
