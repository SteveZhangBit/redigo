package kv

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/SteveZhangBit/redigo/rtype"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type KeySpaceSignalHandler interface {
	SignalModifyKey(key []byte)
	SignalListAsReady(key []byte)
}

type KV struct {
	KeySpaceSignalHandler

	ID      int
	dict    map[string]interface{}
	expires map[string]time.Duration
}

func NewKV(id int) *KV {
	return &KV{
		ID:      id,
		dict:    make(map[string]interface{}),
		expires: make(map[string]time.Duration),
	}
}

func (m *KV) GetSpace() map[string]interface{} {
	return m.dict
}

func (m *KV) LookupKey(key []byte) interface{} {
	/* TODO: Update the access time for the ageing algorithm.
	 * Don't do it if we have a saving child, as this will trigger
	 * a copy on write madness. */

	o, _ := m.dict[string(key)]
	return o
}

func (m *KV) LookupKeyRead(key []byte) (o interface{}, hit bool) {
	m.ExpireIfNeed(key)

	o, hit = m.dict[string(key)]
	return
}

func (m *KV) LookupKeyWrite(key []byte) interface{} {
	m.ExpireIfNeed(key)
	return m.LookupKey(key)
}

/* Add the key to the DB. It's up to the caller to increment the reference
 * counter of the value if needed.
 *
 * The program is aborted if the key already exists. */
func (m *KV) Add(key []byte, val interface{}) {
	if _, ok := m.dict[string(key)]; !ok {
		m.dict[string(key)] = val
		if _, ok = val.(rtype.List); ok {
			m.SignalListAsReady(key)
		}
	} else {
		panic(fmt.Sprintf("The key %s already exists.", key))
	}
}

func (m *KV) Update(key []byte, val interface{}) {
	if _, ok := m.dict[string(key)]; ok {
		m.dict[string(key)] = val
	} else {
		panic(fmt.Sprintf("Key %s doesn't exist", key))
	}
}

func (m *KV) Delete(key []byte) (ok bool) {
	if _, ok = m.expires[string(key)]; ok {
		delete(m.expires, string(key))
	}
	if _, ok = m.dict[string(key)]; ok {
		delete(m.dict, string(key))
	}
	return
}

/* High level Set operation. This function can be used in order to set
 * a key, whatever it was existing or not, to a new object.
 *
 * 1) The ref count of the value object is incremented.
 * 2) clients WATCHing for the destination key notified.
 * 3) The expire time of the key is reset (the key is made persistent). */
func (m *KV) SetKeyPersist(key []byte, val interface{}) {
	if m.LookupKeyWrite(key) == nil {
		m.Add(key, val)
	} else {
		m.dict[string(key)] = val
	}
	m.RemoveExpire(key)
	m.SignalModifyKey(key)
}

func (m *KV) Exists(key []byte) (ok bool) {
	_, ok = m.dict[string(key)]
	return
}

func (m *KV) RandomKey() (key []byte) {
	keys := make([]string, len(m.dict))

	i := 0
	for k := range m.dict {
		keys[i] = k
		i++
	}

	for {
		key = []byte(keys[rand.Intn(len(keys))])
		if m.ExpireIfNeed(key) {
			continue
		}
		return
	}
}

/*-----------------------------------------------------------------------------
 * Expires API
 *----------------------------------------------------------------------------*/

func (m *KV) ExpireIfNeed(key []byte) bool {
	return false
}

func (m *KV) GetExpire(key []byte) (t time.Duration) {
	t, _ = m.expires[string(key)]
	return
}

func (m *KV) SetExpire(key []byte, t time.Duration) {
	m.expires[string(key)] = t
}

func (m *KV) RemoveExpire(key []byte) {
	if _, ok := m.expires[string(key)]; ok {
		delete(m.expires, string(key))
	}
}
