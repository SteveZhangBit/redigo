package rstring

import (
	"bytes"
	"strconv"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/rtype"
)

type BytesString struct {
	Val []byte
}

func (s *BytesString) String() string {
	return string(s.Val)
}

func (s *BytesString) Bytes() []byte {
	return s.Val
}

func (s *BytesString) Len() int64 {
	return int64(len(s.Val))
}

func (s *BytesString) Append(b []byte) rtype.String {
	s.Val = append(s.Val, b...)
	return s
}

type IntString struct {
	Val int64
}

func (i *IntString) String() string {
	return strconv.FormatInt(i.Val, 10)
}

func (i *IntString) Bytes() []byte {
	return []byte(i.String())
}

func (i *IntString) Len() int64 {
	var count int64
	x := i.Val
	for x > 0 {
		count++
		x /= 10
	}
	return count
}

func (i *IntString) Append(b []byte) rtype.String {
	return &BytesString{append(i.Bytes(), b...)}
}

func New(val []byte) rtype.String {
	// Check whether can be convert to integer
	if val[0] == '+' || val[0] == '-' || (val[0] >= '0' && val[0] <= '9') {
		if x, ok := redigo.ParseInt(val, 10, 64); ok {
			return &IntString{x}
		}
	}
	return &BytesString{val}
}

func NewFromInt64(val int64) rtype.String {
	return &IntString{val}
}

func NewFromFloat64(val float64) rtype.String {
	return &BytesString{[]byte(strconv.FormatFloat(val, 'g', 17, 64))}
}

func CompareStringObjects(a, b rtype.String) int {
	x, x_ok := a.(*IntString)
	y, y_ok := b.(*IntString)
	if x_ok && y_ok {
		if x.Val < y.Val {
			return -1
		} else if x.Val > y.Val {
			return 1
		} else {
			return 0
		}
	} else {
		return bytes.Compare(a.Bytes(), b.Bytes())
	}
}

func EqualStringObjects(a, b rtype.String) bool {
	return CompareStringObjects(a, b) == 0
}
