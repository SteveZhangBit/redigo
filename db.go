package redigo

import (
	"fmt"
	"time"
)

type RedigoDB struct {
	ID      int
	Dict    map[string]interface{}
	Expires map[string]time.Time
	Server  *RedigoServer
}

/* Add the key to the DB. It's up to the caller to increment the reference
 * counter of the value if needed.
 *
 * The program is aborted if the key already exists. */
func (r *RedigoDB) Add(key string, val interface{}) {
	if _, ok := r.Dict[key]; !ok {
		r.Dict[key] = val
		// if _, ok = val.(*list.LinkedList); ok {
		// 	r.SignalListAsReady(key)
		// }
	} else {
		panic(fmt.Sprintf("The key %s already exists.", key))
	}
}

func (r *RedigoDB) Delete(key string) {

}

func (r *RedigoDB) SetKey(key string, val interface{}) {

}

func (r *RedigoDB) LookupKey(key string) interface{} {
	/* TODO: Update the access time for the ageing algorithm.
	 * Don't do it if we have a saving child, as this will trigger
	 * a copy on write madness. */

	o, _ := r.Dict[key]
	return o
}

func (r *RedigoDB) LookupKeyRead(key string) interface{} {
	r.expireIfNeed(key)

	if o, ok := r.Dict[key]; !ok {
		r.Server.KeyspaceMisses++
		return nil
	} else {
		r.Server.KeyspaceHits++
		return o
	}
}

func (r *RedigoDB) LookupKeyWrite(key string) interface{} {
	r.expireIfNeed(key)
	return r.LookupKey(key)
}

func (r *RedigoDB) SignalModifyKey(key string) {

}

func (r *RedigoDB) expireIfNeed(key string) {

}
