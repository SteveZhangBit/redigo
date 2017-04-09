package sds

import (
	"bytes"
	"fmt"
)

type SDS []byte

func New(b interface{}) *SDS {
	switch x := b.(type) {
	case []byte:
		sds := SDS(x)
		return &sds
	case string:
		sds := SDS([]byte(x))
		return &sds
	default:
		panic(fmt.Sprintf("Cannot create SDS object from %v", x))
	}
}

func Empty() *SDS {
	sds := SDS([]byte{})
	return &sds
}

func (s *SDS) Free() int {
	free := cap(*s) - len(*s)

	dst := make([]byte, len(*s))
	copy(dst, *s)
	*s = dst

	return free
}

func (s *SDS) Len() int {
	return len(*s)
}

func (s *SDS) Avail() int {
	return cap(*s) - len(*s)
}

func (s *SDS) Dup() *SDS {
	dup := SDS(make([]byte, len(*s)))
	copy(dup, *s)
	return &dup
}

func (s *SDS) Clear() {
	*s = (*s)[:0]
}

func (s *SDS) Cat(b interface{}) {
	switch x := b.(type) {
	case []byte:
		*s = append(*s, x...)
	case string:
		*s = append(*s, []byte(x)...)
	}
}

func (s *SDS) Copy(b interface{}) {
	switch x := b.(type) {
	case []byte:
		if cap(*s) < len(x) {
			*s = x
		} else {
			*s = (*s)[:len(x)]
			copy(*s, x)
		}
	case string:
		s.Copy([]byte(x))
	}
}

func (s *SDS) GrowZero(n int) {
	n = n - s.Len()
	for i := 0; i < n; i++ {
		*s = append(*s, ' ')
	}
}

func (s *SDS) Compare(o *SDS) int {
	return bytes.Compare(*s, *o)
}

func (s *SDS) Trim(b string) {
	*s = bytes.Trim(*s, b)
}
