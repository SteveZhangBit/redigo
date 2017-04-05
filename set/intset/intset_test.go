package intset

import (
	"fmt"
	"testing"
)

func Test(t *testing.T) {
	s := New()
	s.Add(1)
	fmt.Println(s)
	s.Add(4)
	fmt.Println(s)
	s.Add(3)
	fmt.Println(s)

	s.Add(3)
	fmt.Println(s)

	s.Add(2)
	fmt.Println(s)

	s.Add(1 << 17)
	fmt.Println(s)

	s.Add(-(1 << 35))
	fmt.Println(s)

	fmt.Println(s.Random())
	fmt.Println(s.Random())

	s.Remove(4)
	s.Remove(1 << 17)
	s.Remove(-1)
	fmt.Println(s)
}
