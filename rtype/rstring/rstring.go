package rstring

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/SteveZhangBit/redigo/rtype"
)

type BytesString []byte

func (n BytesString) String() string {
	return string(n)
}

func (n BytesString) Bytes() []byte {
	return n
}

func (n BytesString) Len() int64 {
	return int64(len(n))
}

func (n BytesString) Append(b []byte) rtype.String {
	return BytesString(append(n, b...))
}

type IntString int64

func (i IntString) String() string {
	return strconv.FormatInt(int64(i), 10)
}

func (i IntString) Bytes() []byte {
	return []byte(i.String())
}

func (i IntString) Len() int64 {
	var count int64
	for i > 0 {
		count++
		i /= 10
	}
	return count
}

func (i IntString) Append(b []byte) rtype.String {
	return BytesString(append(i.Bytes(), b...))
}

func New(val []byte) rtype.String {
	// Check whether can be convert to integer
	if val[0] == '+' || val[0] == '-' || (val[0] >= '0' && val[0] <= '9') {
		if x, err := strconv.ParseInt(string(val), 10, 64); err == nil {
			return IntString(x)
		}
	}
	return BytesString(val)
}

func NewFromInt64(val int64) rtype.String {
	return IntString(val)
}

func NewFromFloat64(val float64) rtype.String {
	return BytesString(fmt.Sprintf("%.17g", val))
}

func CompareStringObjects(a, b rtype.String) int {
	x, x_ok := a.(IntString)
	y, y_ok := b.(IntString)
	if x_ok && y_ok {
		if x < y {
			return -1
		} else if x > y {
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
