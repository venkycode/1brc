package trie

import (
	"testing"

	"github.com/venkycode/1brc/models"
	"github.com/venkycode/1brc/parser"
)

func TestTrie(x *testing.T) {
	t := NewTrie()

	name := strToByteArray("Abha")
	acc := models.NewAccumulator(name, 100)
	t.Insert(&acc)
	out := make(chan *models.Accumulator, 1024)
	t.Walk(out)
	close(out)

	for acc := range out {
		if acc.Name != &name {
			x.Errorf("Expected %v, got %v", name, acc.Name)
		}
	}
}

func strToByteArray(str string) [150]byte {
	var name [150]byte
	for i, c := range str {
		name[i] = byte(c)
	}
	name[len(str)] = parser.CUSTOM_TERMINATOR
	return name
}
