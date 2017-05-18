package server

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/SteveZhangBit/redigo/rtype"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type RedigoDB struct {
	id     int
	server *RedigoServer

	blockingKeys map[string][]*RedigoClient
	readyKeys    map[string]struct{}

	dict    map[string]interface{}
	expires map[string]time.Duration
}

func NewDB() *RedigoDB {
	return &RedigoDB{
		dict:         make(map[string]interface{}),
		expires:      make(map[string]time.Duration),
		blockingKeys: make(map[string][]*RedigoClient),
		readyKeys:    make(map[string]struct{}),
	}
}

func (r *RedigoDB) GetID() int {
	return r.id
}

func (r *RedigoDB) GetDict() map[string]interface{} {
	return r.dict
}

func (r *RedigoDB) LookupKey(key []byte) interface{} {
	/* TODO: Update the access time for the ageing algorithm.
	 * Don't do it if we have a saving child, as this will trigger
	 * a copy on write madness. */

	o, _ := r.dict[string(key)]
	return o
}

func (r *RedigoDB) LookupKeyRead(key []byte) interface{} {
	r.ExpireIfNeed(key)

	if o, ok := r.dict[string(key)]; !ok {
		r.server.keyspaceMisses++
		return nil
	} else {
		r.server.keyspaceHits++
		return o
	}
}

func (r *RedigoDB) LookupKeyWrite(key []byte) interface{} {
	r.ExpireIfNeed(key)
	return r.LookupKey(key)
}

/* Add the key to the DB. It's up to the caller to increment the reference
 * counter of the value if needed.
 *
 * The program is aborted if the key already exists. */
func (r *RedigoDB) Add(key []byte, val interface{}) {
	if _, ok := r.dict[string(key)]; !ok {
		r.dict[string(key)] = val
		if _, ok = val.(rtype.List); ok {
			r.signalListAsReady(key)
		}
	} else {
		panic(fmt.Sprintf("The key %s already exists.", key))
	}
}

func (r *RedigoDB) Update(key []byte, val interface{}) {
	if _, ok := r.dict[string(key)]; ok {
		r.dict[string(key)] = val
	} else {
		panic(fmt.Sprintf("Key %s doesn't exist", key))
	}
}

func (r *RedigoDB) Delete(key []byte) (ok bool) {
	if _, ok = r.expires[string(key)]; ok {
		delete(r.expires, string(key))
	}
	if _, ok = r.dict[string(key)]; ok {
		delete(r.dict, string(key))
	}
	return
}

/* High level Set operation. This function can be used in order to set
 * a key, whatever it was existing or not, to a new object.
 *
 * 1) The ref count of the value object is incremented.
 * 2) clients WATCHing for the destination key notified.
 * 3) The expire time of the key is reset (the key is made persistent). */
func (r *RedigoDB) SetKeyPersist(key []byte, val interface{}) {
	if r.LookupKeyWrite(key) == nil {
		r.Add(key, val)
	} else {
		r.dict[string(key)] = val
	}
	r.removeExpire(key)
	r.SignalModifyKey(key)
}

func (r *RedigoDB) Exists(key []byte) (ok bool) {
	_, ok = r.dict[string(key)]
	return
}

func (r *RedigoDB) RandomKey() (key []byte) {
	keys := make([]string, len(r.dict))

	i := 0
	for k := range r.dict {
		keys[i] = k
		i++
	}

	for {
		key = []byte(keys[rand.Intn(len(keys))])
		if r.ExpireIfNeed(key) {
			continue
		}
		return
	}
}

/*-----------------------------------------------------------------------------
 * Hooks for key space changes.
 *
 * Every time a key in the database is modified the function
 * signalModifiedKey() is called.
 *
 * Every time a DB is flushed the function signalFlushDb() is called.
 *----------------------------------------------------------------------------*/

func (r *RedigoDB) SignalModifyKey(key []byte) {

}

func (r *RedigoDB) signalListAsReady(key []byte) {
	// No clients blocking for this key? No need to queue it
	if _, ok := r.blockingKeys[string(key)]; !ok {
		return
	}
	// Key was already signaled? No need to queue it again
	if _, ok := r.readyKeys[string(key)]; ok {
		return
	}

	// Ok, we need to queue this key into server.ready_keys
	r.server.readyKeys = append(r.server.readyKeys, ReadyKey{DB: r, Key: key})

	/* We also add the key in the db->ready_keys dictionary in order
	 * to avoid adding it multiple times into a list with a simple O(1)
	 * check. */
	r.readyKeys[string(key)] = struct{}{}
}

/*-----------------------------------------------------------------------------
 * Expires API
 *----------------------------------------------------------------------------*/

func (r *RedigoDB) ExpireIfNeed(key []byte) bool {
	return false
}

func (r *RedigoDB) GetExpire(key []byte) time.Duration {
	return time.Duration(-1)
}

func (r *RedigoDB) SetExpire(key []byte, t time.Duration) {

}

func (r *RedigoDB) removeExpire(key []byte) {

}

// func (r *RedigoDB) setExpire(key string, when time.Time) {

// }

// func (r *RedigoDB) getExpire(key string) time.Time {

// }
