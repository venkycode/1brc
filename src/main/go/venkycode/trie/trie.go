package trie

import (
	"github.com/venkycode/1brc/models"
	"github.com/venkycode/1brc/parser"
)

type Node struct {
	//inputStream chan *record
	children [256]*Node
	data     *models.Accumulator
	//mutex       Mutex
}

func NewTrie() *Node {
	n := newNode(0, nil)
	return n
}

func (t *Node) Insert(acc *models.Accumulator) {
	t.insert(acc.Name, acc, -1)
}

func (t *Node) Walk(out chan<- *models.Accumulator) {
	if t.data != nil {
		out <- t.data

	}
	for _, child := range t.children {
		if child != nil {
			child.Walk(out)
		}
	}
}

// insert name[sindex:] into the trie
func (t *Node) insert(name *[150]byte, acc *models.Accumulator, sindex int) {
	if sindex > -1 && name[sindex] == parser.CUSTOM_TERMINATOR { // complete
		t.data.Merge(acc)
		return
	}
	sindex++
	nextNode := t.children[name[sindex]]
	if nextNode == nil { // no node
		nextNode = newNode(name[sindex], name)
		t.children[name[sindex]] = nextNode // cache
	}
	nextNode.insert(name, acc, sindex)
}

func newNode(frag byte, name *[150]byte) *Node {
	node := &Node{}
	if frag == parser.CUSTOM_TERMINATOR {
		node.data = &models.Accumulator{
			Name:  name,
			Sum:   0,
			Count: 0,
			Min:   200000,
			Max:   -2000000,
		}
	}

	return node
}
