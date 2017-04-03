package zskiplist

import (
	"fmt"

	"github.com/SteveZhangBit/redigo/rstring"

	"testing"
)

func Test(t *testing.T) {
	z := New()
	z.Insert(1.0, rstring.New([]byte("1234")))
	z.Insert(2.0, rstring.New([]byte("123")))
	z.Insert(1.5, rstring.New([]byte("abc")))
	z.Insert(0.5, rstring.New([]byte("111")))
	z.Insert(1.0, rstring.New([]byte("12345")))
	fmt.Println(z)
	fmt.Println(z.GetRank(1.5, rstring.New([]byte("abc"))))
	fmt.Println(z.Delete(1.0, rstring.New([]byte("1234"))))
	fmt.Println(z)
	fmt.Println(z.GetRank(1.5, rstring.New([]byte("abc"))))

	z = New()
	for i := 0; i < 12; i++ {
		z.Insert(float64(i), rstring.New([]byte(fmt.Sprintf("%d", i))))
	}
	fmt.Println(z)
}
