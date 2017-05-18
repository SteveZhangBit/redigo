package set

import (
	"math/rand"
	"time"

	"github.com/SteveZhangBit/redigo/rtype"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
	"github.com/SteveZhangBit/redigo/rtype/set/intset"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type HashSet map[string]struct{}

func (h HashSet) Add(val rtype.String) bool {
	key := val.String()
	if _, ok := h[key]; !ok {
		h[key] = struct{}{}
		return true
	}
	return false
}

func (h HashSet) Remove(val rtype.String) bool {
	if _, ok := h[val.String()]; ok {
		delete(h, val.String())
		return true
	}
	return false
}

func (h HashSet) Size() int {
	return len(h)
}

func (h HashSet) IsMember(val rtype.String) bool {
	_, ok := h[val.String()]
	return ok
}

func (h HashSet) RandomElement() rtype.String {
	var val string
	count, i := rand.Intn(h.Size()), 0
	for val = range h {
		if i < count {
			i++
		} else {
			break
		}
	}
	return rstring.New([]byte(val))
}

type IntsetSet struct {
	s *intset.IntSet
}

func (i *IntsetSet) Add(val rtype.String) bool {
	return i.s.Add(int64(val.(rstring.IntString)))
}

func (i *IntsetSet) Remove(val rtype.String) bool {
	return i.s.Remove(int64(val.(rstring.IntString)))
}

func (i *IntsetSet) Size() int {
	return i.s.Length
}

func (i *IntsetSet) IsMember(val rtype.String) bool {
	return i.s.Find(int64(val.(rstring.IntString)))
}

func (i *IntsetSet) RandomElement() rtype.String {
	return rstring.NewFromInt64(i.s.Random())
}

func (i *IntsetSet) Convert() HashSet {
	hs := make(HashSet)
	for j := 0; j < i.Size(); j++ {
		hs[rstring.NewFromInt64(i.s.Get(j)).String()] = struct{}{}
	}
	return hs
}

func New(val rtype.String) rtype.Set {
	var s rtype.Set
	switch val.(type) {
	case rstring.BytesString:
		s = make(HashSet)
	case rstring.IntString:
		s = &IntsetSet{s: intset.New()}
	}
	return s
}
