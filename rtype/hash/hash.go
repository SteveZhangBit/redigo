package hash

import "github.com/SteveZhangBit/redigo/rtype/rstring"

const (
	HashKey = (1 << iota)
	HashValue
)

type basicMap map[string]*rstring.RString

type HashMap struct {
	Val interface{}
}

func New() *HashMap {
	return &HashMap{Val: make(basicMap)}
}

/* Add an element, discard the old if the key already exists.
 * Return false on insert and true on update. */
func (h *HashMap) Set(key string, val *rstring.RString) (update bool) {
	m := h.Val.(basicMap)
	_, update = m[key]
	m[key] = val
	return
}

func (h *HashMap) Get(key string) (val *rstring.RString, ok bool) {
	val, ok = h.Val.(basicMap)[key]
	return
}

func (h *HashMap) Delete(key string) {
	delete(h.Val.(basicMap), key)
}

func (h *HashMap) Len() int {
	return len(h.Val.(basicMap))
}

func (h *HashMap) Iterate(iterf func(key string, val *rstring.RString)) {
	for key, val := range h.Val.(basicMap) {
		iterf(key, val)
	}
}
