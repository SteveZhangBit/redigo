package redigo

import "github.com/SteveZhangBit/redigo/kv"

type DB struct {
	*kv.KV

	server         *Server
	blockedClients map[string][]*Client
	readyKeys      map[string]struct{}
}

func NewDB(server *Server, id int) *DB {
	return &DB{
		KV:             kv.NewKV(id),
		server:         server,
		blockedClients: make(map[string][]*Client),
		readyKeys:      make(map[string]struct{}),
	}
}

func (d *DB) LookupKeyRead(key []byte) interface{} {
	if x, ok := d.KV.LookupKeyRead(key); !ok {
		d.server.KeyspaceMisses++
		return nil
	} else {
		d.server.KeyspaceHits++
		return x
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

func (d *DB) SignalModifyKey(key []byte) {

}

func (d *DB) SignalListAsReady(key []byte) {
	// No clients blocking for this key? No need to queue it
	if _, ok := d.blockedClients[string(key)]; !ok {
		return
	}
	// Key was already signaled? No need to queue it again
	if _, ok := d.readyKeys[string(key)]; ok {
		return
	}

	// Ok, we need to queue this key into server.ready_keys
	d.server.readyKeys = append(d.server.readyKeys, readyKey{db: d, key: key})

	/* We also add the key in the db->ready_keys dictionary in order
	 * to avoid adding it multiple times into a list with a simple O(1)
	 * check. */
	d.readyKeys[string(key)] = struct{}{}
}
