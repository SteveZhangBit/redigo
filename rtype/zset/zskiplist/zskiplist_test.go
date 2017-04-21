package zskiplist

import (
	"fmt"
	"testing"

	"github.com/SteveZhangBit/redigo/rtype/rstring"
)

func Test(t *testing.T) {
	z := New()
	z.Insert(1.0, rstring.New("12345"))
	z.Insert(1.0, rstring.New("1234"))
	z.Insert(2.0, rstring.New("123"))
	z.Insert(1.5, rstring.New("abc"))
	z.Insert(0.5, rstring.New("111"))
	fmt.Println(z)
	fmt.Println(z.GetRank(1.5, rstring.New("abc")))
	fmt.Println(z.Delete(1.0, rstring.New("1234")))
	fmt.Println(z)
	fmt.Println(z.GetRank(1.5, rstring.New("abc")))

	// z = New()
	// for i := 0; i < 12; i++ {
	// 	z.Insert(float64(i), rstring.New([]byte(fmt.Sprintf("%d", i))))
	// }
	// fmt.Println(z)
}