package trie

import (
	"github.com/venkycode/1brc/models"
	"github.com/venkycode/1brc/parser"
)

// this trie will use slices instead of pointers
type Trie struct {
	store []node
}

type node struct {
	children [256]int
	data     *models.Accumulator
}

func NewFlatTrie() *Trie {
	t := &Trie{
		store: make([]node, 0, 1024),
	}
	t.store = append(t.store, newFlatNode(0, nil))

	return t
}

func (t *Trie) Insert(acc models.Accumulator) {
	t.insert(0, acc.Name, acc, -1)
}

func (t *Trie) Walk(out chan<- models.Accumulator) {
	for _, n := range t.store {
		if n.data != nil {
			out <- *n.data
		}
	}
}

func (t *Trie) WalkInOrder(out chan<- models.Accumulator) {
	t.walkInOrder(0, out)
}

func (t *Trie) walkInOrder(tindex int, out chan<- models.Accumulator) {
	if t.store[tindex].data != nil {
		out <- *t.store[tindex].data
	}

	for _, child := range t.store[tindex].children {
		if child != 0 {
			t.walkInOrder(child, out)
		}
	}
}

func (t *Trie) insert(tindex int, name *[150]byte, acc models.Accumulator, sindex int) {
	if sindex > -1 && name[sindex] == parser.CUSTOM_TERMINATOR { // complete
		t.store[tindex].data.Merge(&acc)
		return
	}
	sindex++
	nextNodeIdx := t.store[tindex].children[name[sindex]]
	if nextNodeIdx == 0 { // no node
		nextNodeIdx = len(t.store)
		t.store[tindex].children[name[sindex]] = nextNodeIdx
		t.store = append(t.store, newFlatNode(name[sindex], name))
	}

	t.insert(nextNodeIdx, name, acc, sindex)
}

func newFlatNode(frag byte, name *[150]byte) node {
	n := node{}
	if frag == parser.CUSTOM_TERMINATOR {
		n.data = &models.Accumulator{
			Name:  name,
			Sum:   0,
			Count: 0,
			Min:   200000,
			Max:   -2000000,
		}
	}
	return n
}
