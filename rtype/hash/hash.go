package hash

import "github.com/SteveZhangBit/redigo/rtype"

type BasicMap map[string]rtype.String

func (b BasicMap) Set(key string, val rtype.String) (update bool) {
	_, update = b[key]
	b[key] = val
	return
}

func (b BasicMap) Get(key string) (val rtype.String, ok bool) {
	val, ok = b[key]
	return
}

func (b BasicMap) Delete(key string) {
	delete(b, key)
}

func (b BasicMap) Len() int {
	return len(b)
}

func (b BasicMap) Iterate(iterf func(key string, val rtype.String)) {
	for key, val := range b {
		iterf(key, val)
	}
}

func New() rtype.HashMap {
	return make(BasicMap)
}
