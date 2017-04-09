package rstring

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"unicode/utf8"
)

type RString struct {
	// Could be *SDS or int64 for integer strings
	Val interface{}
}

func New(val string) *RString {
	// Check whether can be convert to integer
	if x, err := strconv.ParseInt(val, 10, 64); err != nil {
		return &RString{Val: x}
	}
	return &RString{Val: val}
}

func NewFromInt64(val int64) *RString {
	return &RString{Val: val}
}

func NewFromFloat64(val float64) *RString {
	return &RString{Val: fmt.Sprintf("%.17f", val)}
}

func (s *RString) String() string {
	switch x := s.Val.(type) {
	case string:
		if utf8.Valid([]byte(x)) {
			return x
		} else {
			return hex.EncodeToString([]byte(x))
		}
	case int64:
		return strconv.FormatInt(x, 10)
	default:
		panic(fmt.Sprintf("Type %T is not a string object", x))
	}
}

func (s *RString) Bytes() []byte {
	switch x := s.Val.(type) {
	case string:
		return []byte(x)
	case int64:
		return []byte(strconv.FormatInt(x, 10))
	default:
		panic(fmt.Sprintf("Type %T is not a string object", x))
	}
}

func (r *RString) Len() int64 {
	switch x := r.Val.(type) {
	case string:
		return int64(len(x))
	case int64:
		return int64(len(strconv.FormatInt(x, 10)))
	default:
		panic(fmt.Sprintf("Type %T is not a string object", x))
	}
}

func (r *RString) Append(b string) {
	switch x := r.Val.(type) {
	case string:
		r.Val = string(append([]byte(x), []byte(b)...))
	case int64:
		r.Val = string(append([]byte(strconv.FormatInt(x, 10)), []byte(b)...))
	default:
		panic(fmt.Sprintf("Type %T is not a string object", x))
	}
}

func CompareStringObjects(a, b *RString) int {
	x, x_ok := a.Val.(int64)
	y, y_ok := b.Val.(int64)
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

func EqualStringObjects(a, b *RString) bool {
	return CompareStringObjects(a, b) == 0
}
