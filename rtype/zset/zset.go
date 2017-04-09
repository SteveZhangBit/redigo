package zset

import (
	"github.com/SteveZhangBit/redigo/rtype/rstring"
	"github.com/SteveZhangBit/redigo/rtype/zset/zskiplist"
)

// This package is the same of ZSETs in redis. The following instruction is copied from t_zset.c.

/*-----------------------------------------------------------------------------
 * Sorted set API
 *----------------------------------------------------------------------------*/

/* ZSETs are ordered sets using two data structures to hold the same elements
 * in order to get O(log(N)) INSERT and REMOVE operations into a sorted
 * data structure.
 *
 * The elements are added to a hash table mapping Redis objects to scores.
 * At the same time the elements are added to a skip list mapping scores
 * to Redis objects (so objects are sorted by scores in this "view"). */

type ZSet struct {
	// Currently, it should only be skiplist
	Val interface{}
	// A map to store the keys
	Dict map[rstring.RString]float64
}

func New() *ZSet {
	return &ZSet{Val: zskiplist.New()}
}
