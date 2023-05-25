package record_manager

import "strconv"

type Constant struct {
	iVal  int
	sVal  string
	isInt bool
}

func NewConstantWithInt(val int) *Constant {
	return &Constant{
		iVal:  val,
		isInt: true,
	}
}

func NewConstantWithString(val string) *Constant {
	return &Constant{
		sVal:  val,
		isInt: false,
	}
}

func (c *Constant) AsInt() int {
	return c.iVal
}

func (c *Constant) AsString() string {
	return c.sVal
}

func (c *Constant) Equals(other *Constant) bool {
	if c.isInt {
		return c.iVal == other.iVal
	}

	return c.sVal == other.sVal
}

func (c *Constant) CompareTo(other *Constant) int {
	if c.isInt {
		return c.iVal - other.iVal
	}

	if c.sVal == other.sVal {
		return 0
	} else if c.sVal > other.sVal {
		return 1
	}
	return -1
}

func (c *Constant) ToString() string {
	if c.isInt {
		return strconv.Itoa(c.iVal)
	}

	return c.sVal
}
