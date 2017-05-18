package hash

import "github.com/SteveZhangBit/redigo/rtype"

type BasicMap map[string]rtype.String

func (b BasicMap) Set(key []byte, val rtype.String) (update bool) {
	_, update = b[string(key)]
	b[string(key)] = val
	return
}

func (b BasicMap) Get(key []byte) (val rtype.String, ok bool) {
	val, ok = b[string(key)]
	return
}

func (b BasicMap) Delete(key []byte) {
	delete(b, string(key))
}

func (b BasicMap) Len() int {
	return len(b)
}

func (b BasicMap) Iterate(iterf func(key []byte, val rtype.String)) {
	for key, val := range b {
		iterf([]byte(key), val)
	}
}

func New() rtype.HashMap {
	return make(BasicMap)
}
