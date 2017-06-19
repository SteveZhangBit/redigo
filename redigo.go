package redigo

import (
	"time"

	"github.com/SteveZhangBit/redigo/protocol"
)

const (
	Version = "0.0.1"
)

const (
	REDIS_NOTIFY_STRING = iota
	REDIS_NOTIFY_LIST
	REDIS_NOTIFY_HASH
	REDIS_NOTIFY_SET
	REDIS_NOTIFY_ZSET
	REDIS_NOTIFY_GENERIC
)

/* With multiplexing we need to take per-client state.
 * Clients are taken in a linked list. */
type Client interface {
	protocol.Reader
	protocol.Writer
	PubSub

	DB() DB
	Server() Server
	SelectDB(id int) bool

	LookupKeyReadOrReply(key []byte, reply []byte) interface{}
	LookupKeyWriteOrReply(key []byte, reply []byte) interface{}

	BlockForKeys(keys [][]byte, timeout time.Duration)
}

type Server interface {
	PrepareForShutdown() bool
	AddDirty(i int)
}

/* Redis database representation. There are multiple databases identified
 * by integers from 0 (the default database) up to the max configured
 * database. The database number is the 'id' field in the structure. */
type DB interface {
	GetID() int
	GetDict() map[string]interface{}

	LookupKey(key []byte) interface{}
	LookupKeyRead(key []byte) interface{}
	LookupKeyWrite(key []byte) interface{}

	Add(key []byte, val interface{})
	Update(key []byte, val interface{})
	Delete(key []byte) (ok bool)
	SetKeyPersist(key []byte, val interface{})
	Exists(key []byte) (ok bool)
	RandomKey() (key []byte)

	SignalModifyKey(key []byte)

	ExpireIfNeed(key []byte) bool
	GetExpire(key []byte) time.Duration
	SetExpire(key []byte, t time.Duration)
}

type PubSub interface {
	NotifyKeyspaceEvent(t int, event string, key []byte, dbid int)
}

type CommandArg struct {
	Client
	Argv [][]byte
	Argc int
}
