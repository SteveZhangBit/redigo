package intset

import (
	"fmt"
	"math"
	"math/rand"
	"time"
	"unsafe"
)

const (
	IntSetEncodeInt16 = 2
	IntSetEncodeInt32 = 4
	IntSetEncodeInt64 = 8
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type IntSet struct {
	// This should be one of []int16, []int32, []int64
	Data     interface{}
	Encoding int
	Length   int
}

func New() *IntSet {
	return &IntSet{Data: []int16{}, Encoding: IntSetEncodeInt16}
}

func valueEncoding(x int64) int {
	if x < math.MinInt32 || x > math.MaxInt32 {
		return IntSetEncodeInt64
	} else if x < math.MinInt16 || x > math.MaxInt16 {
		return IntSetEncodeInt32
	} else {
		return IntSetEncodeInt16
	}
}

func (i *IntSet) Get(pos int) int64 {
	switch x := i.Data.(type) {
	case []int16:
		return int64(x[pos])
	case []int32:
		return int64(x[pos])
	case []int64:
		return x[pos]
	default:
		panic(fmt.Sprintf("Wrong type for intset %T", x))
	}
}

func (i *IntSet) Set(pos int, val int64) {
	switch x := i.Data.(type) {
	case []int16:
		x[pos] = int16(val)
	case []int32:
		x[pos] = int32(val)
	case []int64:
		x[pos] = int64(val)
	}
}

func (i *IntSet) MoveTail(from, to int) {
	switch x := i.Data.(type) {
	case []int16:
		i.Data = append(x[:to], x[from:i.Length]...)
	case []int32:
		i.Data = append(x[:to], x[from:i.Length]...)
	case []int64:
		i.Data = append(x[:to], x[from:i.Length]...)
	}
}

func (i *IntSet) Resize(size int) {
	switch x := i.Data.(type) {
	case []int16:
		if len(x) > size {
			i.Data = x[:size]
		} else {
			for len(x) < size {
				x = append(x, math.MinInt16)
			}
			i.Data = x
		}

	case []int32:
		if len(x) > size {
			i.Data = x[:size]
		} else {
			for len(x) < size {
				x = append(x, math.MinInt32)
			}
			i.Data = x
		}

	case []int64:
		if len(x) > size {
			i.Data = x[:size]
		} else {
			for len(x) < size {
				x = append(x, math.MinInt64)
			}
			i.Data = x
		}
	}
}

func (i *IntSet) Add(val int64) bool {
	/* Upgrade encoding if necessary. If we need to upgrade, we know that
	 * this value should be either appended (if > 0) or prepended (if < 0),
	 * because it lies outside the range of existing values. */
	if valueEncoding(val) > i.Encoding {
		return i.UpgradeAndAdd(val)
	} else {
		/* Abort if the value is already present in the set.
		 * This call will populate "pos" with the right position to insert
		 * the value when it cannot be found. */
		pos, ok := i.Search(val)
		if ok {
			return false
		}

		i.Resize(i.Length + 1)
		if pos < i.Length {
			i.MoveTail(pos, pos+1)
		}
		i.Set(pos, val)
		i.Length++
		return true
	}
}

func (i *IntSet) Search(val int64) (int, bool) {
	if i.Length == 0 {
		return 0, false
	} else {
		/* Check for the case where we know we cannot find the value,
		 * but do know the insert position. */
		if val > i.Get(i.Length-1) {
			return i.Length, false
		} else if val < i.Get(0) {
			return 0, false
		}
	}

	// Because the values are stored in order, so we can use binary search
	var min, max, mid int = 0, i.Length - 1, -1
	var cur int64 = -1
	for max >= min {
		mid = int((uint(min) + uint(max)) >> 1) // Pay attention to avoid the integer overflow.
		cur = i.Get(mid)
		if val > cur {
			min = mid + 1
		} else if val < cur {
			max = mid - 1
		} else {
			break
		}
	}
	if val == cur {
		return mid, true
	} else {
		return min, false
	}
}

func (i *IntSet) UpgradeAndAdd(val int64) bool {
	newenc := valueEncoding(val)
	prepend := 0
	if val < 0 {
		prepend = 1
	}

	// First set new encoding and resize
	i.Encoding = newenc

	/* Upgrade back-to-front so we don't overwrite values.
	 * Note that the "prepend" variable is used to make sure we have an empty
	 * space at either the beginning or the end of the intset. */
	switch newenc {
	case IntSetEncodeInt32:
		newslice := make([]int32, i.Length+1)
		for i, x := range i.Data.([]int16) {
			newslice[i+prepend] = int32(x)
		}
		i.Data = newslice

	case IntSetEncodeInt64:
		newslice := make([]int64, i.Length+1)
		switch oldslice := i.Data.(type) {
		case []int16:
			for i, x := range oldslice {
				newslice[i+prepend] = int64(x)
			}
		case []int32:
			for i, x := range oldslice {
				newslice[i+prepend] = int64(x)
			}
		}
		i.Data = newslice
	}

	if prepend == 0 {
		i.Set(i.Length, val)
	} else {
		i.Set(0, val)
	}
	i.Length++
	return true
}

func (i *IntSet) Remove(val int64) bool {
	if valueEncoding(val) <= i.Encoding {
		if pos, ok := i.Search(val); ok {
			i.MoveTail(pos+1, pos)
			i.Length--
			return true
		}
	}
	return false
}

func (i *IntSet) Find(val int64) bool {
	if valueEncoding(val) <= i.Encoding {
		_, ok := i.Search(val)
		return ok
	}
	return false
}

func (i *IntSet) Random() int64 {
	return i.Get(rand.Intn(i.Length))
}

func (i *IntSet) BlobSize() int {
	return int(unsafe.Sizeof(*i)) + i.Encoding*i.Length
}
