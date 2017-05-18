package zset

import (
	"github.com/SteveZhangBit/redigo/rtype"
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

type ZSetSkiplistItem struct {
	e *zskiplist.ZSkiplistNode
}

func (z *ZSetSkiplistItem) Next() rtype.ZSetItem {
	if ln := z.e.Level[0].Forward; ln != nil {
		return &ZSetSkiplistItem{e: ln}
	}
	return nil
}

func (z *ZSetSkiplistItem) Prev() rtype.ZSetItem {
	if ln := z.e.Backward; ln != nil {
		return &ZSetSkiplistItem{e: ln}
	}
	return nil
}

func (z *ZSetSkiplistItem) Value() rtype.String {
	return z.e.Obj
}

func (z *ZSetSkiplistItem) Score() float64 {
	return z.e.Score
}

type ZSetSkiplist struct {
	zsl  *zskiplist.ZSkiplist
	dict map[string]float64
}

func (z *ZSetSkiplist) Add(score float64, v rtype.String) bool {
	if z.zsl.Insert(score, v) != nil {
		z.dict[v.String()] = score
		return true
	}
	return false
}

func (z *ZSetSkiplist) Update(score float64, v rtype.String) bool {
	if z.zsl.Delete(score, v) && z.zsl.Insert(score, v) != nil {
		z.dict[v.String()] = score
		return true
	}
	return false
}

func (z *ZSetSkiplist) Get(v rtype.String) (float64, bool) {
	score, ok := z.dict[v.String()]
	return score, ok
}

func (z *ZSetSkiplist) Delete(score float64, v rtype.String) bool {
	if z.zsl.Delete(score, v) {
		delete(z.dict, v.String())
		return true
	}
	return false
}

func (z *ZSetSkiplist) Len() int {
	return len(z.dict)
}

func (z *ZSetSkiplist) Head() rtype.ZSetItem {
	if ln := z.zsl.Header.Level[0].Forward; ln != nil {
		return &ZSetSkiplistItem{e: ln}
	}
	return nil
}

func (z *ZSetSkiplist) Tail() rtype.ZSetItem {
	if ln := z.zsl.Tail; ln != nil {
		return &ZSetSkiplistItem{e: ln}
	}
	return nil
}

func (z *ZSetSkiplist) GetByRank(rank uint) rtype.ZSetItem {
	if ln := z.zsl.GetElementByRank(rank); ln != nil {
		return &ZSetSkiplistItem{e: ln}
	}
	return nil
}

func (z *ZSetSkiplist) GetRank(score float64, v rtype.String) uint {
	return z.zsl.GetRank(score, v)
}

func New() rtype.ZSet {
	return &ZSetSkiplist{zsl: zskiplist.New(), dict: make(map[string]float64)}
}
