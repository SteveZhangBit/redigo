package set

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
	"github.com/SteveZhangBit/redigo/rtype/set/intset"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type HashSet map[rstring.RString]struct{}

type Set struct {
	// Should be intset or hashtable
	Val interface{}
}

func New() *Set {
	return &Set{}
}

func (s *Set) convert() {
	switch x := s.Val.(type) {
	case *intset.IntSet:
		new_s := make(HashSet)
		for i := 0; i < x.Length; i++ {
			new_s[*rstring.NewFromInt64(x.Get(i))] = struct{}{}
		}
		s.Val = new_s
	default:
		panic("Unsupported set conversion")
	}
}

func (s *Set) Add(val *rstring.RString) bool {
	switch s_enc := s.Val.(type) {
	case *intset.IntSet:
		if x, ok := val.Val.(int64); ok {
			if s_enc.Add(x) {
				// Convert to regular set when the intset contains too many entries.
				if s_enc.Length > redigo.MaxIntsetEntries {
					s.convert()
				}
				return true
			}
		} else {
			// Failed to get integer from object, convert to regular set.
			s.convert()
			return s.Add(val)
		}

	case HashSet:
		if _, ok := s_enc[*val]; ok {
			return true
		}

	default:
		panic(fmt.Sprintf("Type %T is not a set object", s_enc))
	}
	return false
}

func (s *Set) Remove(val *rstring.RString) bool {
	switch s_enc := s.Val.(type) {
	case *intset.IntSet:
		if x, ok := val.Val.(int64); ok {
			return s_enc.Remove(x)
		}

	case HashSet:
		if _, ok := s_enc[*val]; ok {
			delete(s_enc, *val)
			return true
		}

	default:
		panic(fmt.Sprintf("Type %T is not a set object", s_enc))
	}
	return false
}

func (s *Set) Size() int {
	switch s_enc := s.Val.(type) {
	case *intset.IntSet:
		return s_enc.Length

	case HashSet:
		return len(s_enc)

	default:
		panic(fmt.Sprintf("Type %T is not a set object", s_enc))
	}
}

func (s *Set) IsMember(val *rstring.RString) bool {
	switch s_enc := s.Val.(type) {
	case *intset.IntSet:
		if x, ok := val.Val.(int64); ok {
			return s_enc.Find(x)
		}

	case HashSet:
		_, ok := s_enc[*val]
		return ok

	default:
		panic(fmt.Sprintf("Type %T is not a set object", s_enc))
	}
	return false
}

func (s *Set) RandomElement() *rstring.RString {
	switch s_enc := s.Val.(type) {
	case *intset.IntSet:
		return rstring.NewFromInt64(s_enc.Random())

	case HashSet:
		count := rand.Intn(len(s_enc))
		i := 0
		for val := range s_enc {
			if i < count {
				count++
			} else {
				return &rstring.RString{Val: val.Val}
			}
		}

	default:
		panic(fmt.Sprintf("Type %T is not a set object", s_enc))
	}
	return nil
}
