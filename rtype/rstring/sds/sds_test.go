package sds

import (
	"fmt"
	"testing"
)

func print(s *SDS) {
	fmt.Printf("addr: %p, internal addr: %p, len: %d, cap: %d, val: \"%v\"\n", s, *s, len(*s), cap(*s), string(*s))
}

func Test(t *testing.T) {
	s := New("abcd")
	print(s)
	fmt.Printf("Len: %d, Avail: %d\n", s.Len(), s.Avail())

	dup := s.Dup()
	print(dup)

	s.Clear()
	print(s)

	s.Free()
	print(s)

	s.Cat("1234")
	print(s)

	s.Copy("abc")
	print(s)

	s.Copy("1234abcde")
	print(s)

	s.GrowZero(13)
	print(s)

	fmt.Printf("Compare: %d", s.Compare(New("1234abcde")))

	s.Trim(" ")
	print(s)
}
