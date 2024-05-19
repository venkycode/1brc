package models

type Accumulator struct {
	Name  *[150]byte
	Sum   int64
	Count int64
	Min   int64
	Max   int64
}

func (acc *Accumulator) Merge(in *Accumulator) {
	acc.Count += in.Count
	acc.Sum += in.Sum
	if acc.Min > in.Min {
		acc.Min = in.Min
	}
	if acc.Max < in.Max {
		acc.Max = in.Max
	}
}

func NewAccumulator(name [150]byte, temperature int64) Accumulator {
	return Accumulator{
		Name:  &name,
		Sum:   temperature,
		Count: 1,
		Min:   temperature,
		Max:   temperature,
	}
}
