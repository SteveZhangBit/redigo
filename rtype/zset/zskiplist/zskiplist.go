package zskiplist

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/SteveZhangBit/redigo/rtype/rstring"
)

/* This skiplist implementation is almost a C translation of the original
 * algorithm described by William Pugh in "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees", modified in three ways:
 * a) this implementation allows for repeated scores.
 * b) the comparison is not just by key (our 'score') but by satellite data.
 * c) there is a back pointer, so it's a doubly linked list with the back
 * pointers being only at "level 1". This allows to traverse the list
 * from tail to head, useful for ZREVRANGE. */

const (
	ZSkiplistMaxLevel = 32
	ZSkiplist_P       = 0.25
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type ZSkiplistLevel struct {
	Forward *ZSkiplistNode
	Span    uint
}

func (z ZSkiplistLevel) String() string {
	if z.Forward != nil {
		return fmt.Sprintf("<%v, %d>", z.Forward.Obj, z.Span)
	} else {
		return "<nil>"
	}
}

type ZSkiplistNode struct {
	Obj      *rstring.RString
	Score    float64
	Backward *ZSkiplistNode
	Level    []ZSkiplistLevel
}

func (z *ZSkiplistNode) String() string {
	return fmt.Sprintf("{obj: %s, score: %.3f, levels: %v}", z.Obj, z.Score, z.Level)
}

type ZSkiplist struct {
	Header, Tail *ZSkiplistNode
	Length       uint
	Level        int
}

func New() *ZSkiplist {
	return &ZSkiplist{
		Header: &ZSkiplistNode{Level: make([]ZSkiplistLevel, ZSkiplistMaxLevel)},
		Level:  1,
	}
}

func (z *ZSkiplist) String() string {
	objs := make([]string, z.Length+1)
	i := 0
	for x := z.Header; x != nil; x = x.Level[0].Forward {
		objs[i] = x.String()
		i++
	}
	return fmt.Sprintf("[\n\t%s\n]", strings.Join(objs, ",\n\t"))
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
func (z *ZSkiplist) randomLevel() int {
	level := 1
	for float64(rand.Int()&0xFFFF) < (ZSkiplist_P * 0xFFFF) {
		level++
	}
	if level < ZSkiplistMaxLevel {
		return level
	} else {
		return ZSkiplistMaxLevel
	}
}

func (z *ZSkiplist) Insert(score float64, obj *rstring.RString) *ZSkiplistNode {
	var update [ZSkiplistMaxLevel]*ZSkiplistNode
	var rank [ZSkiplistMaxLevel]uint

	x := z.Header
	for i := z.Level - 1; i >= 0; i-- {
		// store rank that is crossed to reach the insert position
		if i == z.Level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.Level[i].Forward != nil && (x.Level[i].Forward.Score < score ||
			(x.Level[i].Forward.Score == score && rstring.CompareStringObjects(x.Level[i].Forward.Obj, obj) < 0)) {
			rank[i] += x.Level[i].Span
			x = x.Level[i].Forward
		}
		update[i] = x
	}
	/* we assume the key is not already inside, since we allow duplicated
	 * scores, and the re-insertion of score and redis object should never
	 * happen since the caller of zslInsert() should test in the hash table
	 * if the element is already inside or not. */
	level := z.randomLevel()
	if level > z.Level {
		for i := z.Level; i < level; i++ {
			rank[i] = 0
			update[i] = z.Header
			update[i].Level[i].Span = z.Length
		}
		z.Level = level
	}
	x = &ZSkiplistNode{Level: make([]ZSkiplistLevel, level), Score: score, Obj: obj}
	for i := 0; i < level; i++ {
		x.Level[i].Forward = update[i].Level[i].Forward
		update[i].Level[i].Forward = x

		// update span covered by update[i] as x is inserted here
		x.Level[i].Span = update[i].Level[i].Span - (rank[0] - rank[i])
		update[i].Level[i].Span = (rank[0] - rank[i]) + 1
	}

	// increment span for untouched levels
	for i := level; i < z.Level; i++ {
		update[i].Level[i].Span++
	}

	if update[0] != z.Header {
		x.Backward = update[0]
	}
	if x.Level[0].Forward != nil {
		x.Level[0].Forward.Backward = x
	} else {
		z.Tail = x
	}
	z.Length++
	return x
}

func (z *ZSkiplist) deletNode(x *ZSkiplistNode, update []*ZSkiplistNode) {
	for i := 0; i < z.Level; i++ {
		if update[i].Level[i].Forward == x {
			update[i].Level[i].Span += x.Level[i].Span - 1
			update[i].Level[i].Forward = x.Level[i].Forward
		} else {
			update[i].Level[i].Span -= 1
		}
	}
	if x.Level[0].Forward != nil {
		x.Level[0].Forward.Backward = x.Backward
	} else {
		z.Tail = x.Backward
	}
	for z.Level > 1 && z.Header.Level[z.Level-1].Forward == nil {
		z.Level--
	}
	z.Length--
}

func (z *ZSkiplist) Delete(score float64, obj *rstring.RString) bool {
	var update [ZSkiplistMaxLevel]*ZSkiplistNode

	x := z.Header
	for i := z.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil && (x.Level[i].Forward.Score < score ||
			(x.Level[i].Forward.Score == score && rstring.CompareStringObjects(x.Level[i].Forward.Obj, obj) < 0)) {
			x = x.Level[i].Forward
		}
		update[i] = x
	}
	/* We may have multiple elements with the same score, what we need
	 * is to find the element with both the right score and object. */
	x = x.Level[0].Forward
	if x != nil && score == x.Score && rstring.EqualStringObjects(x.Obj, obj) {
		z.deletNode(x, update[:])
		return true
	}
	return false
}

// func (z *ZSkiplist) FirstInRange(zrangespec *range) {

// }

// func (z *ZSkiplist) LastInRange(zrangespec *range) {

// }

/* Find the rank for an element by both score and key.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element. */
func (z *ZSkiplist) GetRank(score float64, obj *rstring.RString) (rank uint) {
	x := z.Header
	for i := z.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil && (x.Level[i].Forward.Score < score ||
			(x.Level[i].Forward.Score == score && rstring.CompareStringObjects(x.Level[i].Forward.Obj, obj) < 0)) {
			rank += x.Level[i].Span
			x = x.Level[i].Forward
		}
		// x might be equal to zsl->header, so test if obj is non-NULL
		if x.Obj != nil && x.Obj == obj {
			return
		}
	}
	return
}

/* Finds an element by its rank. The rank argument needs to be 1-based. */
func (z *ZSkiplist) GetElementByRank(rank uint) *ZSkiplistNode {
	var traverse uint = 0

	x := z.Header
	for i := z.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil && traverse+x.Level[i].Span <= rank {
			traverse += x.Level[i].Span
			x = x.Level[i].Forward
		}
		if traverse == rank {
			return x
		}
	}
	return nil
}
