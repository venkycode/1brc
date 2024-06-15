package models

type Accumulator struct {
	Name  [150]byte
	Sum   int64
	Count int64
	Min   int64
	Max   int64
}

func (a Accumulator) Merge(b Accumulator) Accumulator {
	a.Sum += b.Sum
	a.Count += b.Count
	if a.Min > b.Min {
		a.Min = b.Min
	}
	if a.Max < b.Max {
		a.Max = b.Max
	}
	return a
}

func NewWithoutName(t int64) Accumulator {
	return Accumulator{
		Sum:   t,
		Count: 1,
		Min:   t,
		Max:   t,
	}
}

func New(name [150]byte, t int64) Accumulator {
	return Accumulator{
		Name:  name,
		Sum:   t,
		Count: 1,
		Min:   t,
		Max:   t,
	}
}
