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

func (r *RedigoDB) LookupKey(key string) interface{} {
	/* TODO: Update the access time for the ageing algorithm.
	 * Don't do it if we have a saving child, as this will trigger
	 * a copy on write madness. */

	o, _ := r.dict[key]
	return o
}

func (r *RedigoDB) LookupKeyRead(key string) interface{} {
	r.ExpireIfNeed(key)

	if o, ok := r.dict[key]; !ok {
		r.server.keyspaceMisses++
		return nil
	} else {
		r.server.keyspaceHits++
		return o
	}
}

func (r *RedigoDB) LookupKeyWrite(key string) interface{} {
	r.ExpireIfNeed(key)
	return r.LookupKey(key)
}

/* Add the key to the DB. It's up to the caller to increment the reference
 * counter of the value if needed.
 *
 * The program is aborted if the key already exists. */
func (r *RedigoDB) Add(key string, val interface{}) {
	if _, ok := r.dict[key]; !ok {
		r.dict[key] = val
		if _, ok = val.(rtype.List); ok {
			r.signalListAsReady(key)
		}
	} else {
		panic(fmt.Sprintf("The key %s already exists.", key))
	}
}

func (r *RedigoDB) Update(key string, val interface{}) {
	if _, ok := r.dict[key]; ok {
		r.dict[key] = val
	} else {
		panic(fmt.Sprintf("Key %s doesn't exist", key))
	}
}

func (r *RedigoDB) Delete(key string) (ok bool) {
	if _, ok = r.expires[key]; ok {
		delete(r.expires, key)
	}
	if _, ok = r.dict[key]; ok {
		delete(r.dict, key)
	}
	return
}

/* High level Set operation. This function can be used in order to set
 * a key, whatever it was existing or not, to a new object.
 *
 * 1) The ref count of the value object is incremented.
 * 2) clients WATCHing for the destination key notified.
 * 3) The expire time of the key is reset (the key is made persistent). */
func (r *RedigoDB) SetKeyPersist(key string, val interface{}) {
	if r.LookupKeyWrite(key) == nil {
		r.Add(key, val)
	} else {
		r.dict[key] = val
	}
	r.removeExpire(key)
	r.SignalModifyKey(key)
}

func (r *RedigoDB) Exists(key string) (ok bool) {
	_, ok = r.dict[key]
	return
}

func (r *RedigoDB) RandomKey() (key string) {
	keys := make([]string, len(r.dict))

	i := 0
	for k := range r.dict {
		keys[i] = k
		i++
	}

	for {
		key = keys[rand.Intn(len(keys))]
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

func (r *RedigoDB) SignalModifyKey(key string) {

}

func (r *RedigoDB) signalListAsReady(key string) {
	// No clients blocking for this key? No need to queue it
	if _, ok := r.blockingKeys[key]; !ok {
		return
	}
	// Key was already signaled? No need to queue it again
	if _, ok := r.readyKeys[key]; ok {
		return
	}

	// Ok, we need to queue this key into server.ready_keys
	rk := ReadyKey{DB: r, Key: key}
	r.server.readyKeys = append(r.server.readyKeys, rk)

	/* We also add the key in the db->ready_keys dictionary in order
	 * to avoid adding it multiple times into a list with a simple O(1)
	 * check. */
	r.readyKeys[key] = struct{}{}
}

/*-----------------------------------------------------------------------------
 * Expires API
 *----------------------------------------------------------------------------*/

func (r *RedigoDB) ExpireIfNeed(key string) bool {
	return false
}

func (r *RedigoDB) GetExpire(key string) time.Duration {
	return time.Duration(-1)
}

func (r *RedigoDB) SetExpire(key string, t time.Duration) {

}

func (r *RedigoDB) removeExpire(key string) {

}

// func (r *RedigoDB) setExpire(key string, when time.Time) {

// }

// func (r *RedigoDB) getExpire(key string) time.Time {

// }
