package rstring

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/SteveZhangBit/redigo/rtype"
)

type NormString string

func (n NormString) String() string {
	return string(n)
}

func (n NormString) Bytes() []byte {
	return []byte(n)
}

func (n NormString) Len() int64 {
	return int64(len(n))
}

func (n NormString) Append(b string) rtype.String {
	return NormString(append([]byte(n), []byte(b)...))
}

type IntString int64

func (i IntString) String() string {
	return strconv.FormatInt(int64(i), 10)
}

func (i IntString) Bytes() []byte {
	return []byte(strconv.FormatInt(int64(i), 10))
}

func (i IntString) Len() int64 {
	var count int64
	for i > 0 {
		count++
		i /= 10
	}
	return count
}

func (i IntString) Append(b string) rtype.String {
	return NormString(append([]byte(strconv.FormatInt(int64(i), 10)), []byte(b)...))
}

func New(val string) rtype.String {
	// Check whether can be convert to integer
	if x, err := strconv.ParseInt(val, 10, 64); err != nil {
		return IntString(x)
	}
	return NormString(val)
}

func NewFromInt64(val int64) rtype.String {
	return IntString(val)
}

func NewFromFloat64(val float64) rtype.String {
	return NormString(fmt.Sprintf("%.17f", val))
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
