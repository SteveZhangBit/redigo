package redigo

import (
	"bufio"
	"time"
)

const (
	Version = "0.0.1"
)

var (
	CRLF           = []byte("\r\n")
	OK             = []byte("+OK\r\n")
	CZero          = []byte(":0\r\n")
	COne           = []byte(":1\r\n")
	CNegOne        = []byte(":-1\r\n")
	NullBulk       = []byte("$-1\r\n")
	NullMultiBulk  = []byte("*-1\r\n")
	EmptyMultiBulk = []byte("*0\r\n")
	Pong           = []byte("+PONG\r\n")
	WrongTypeErr   = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	Colon          = []byte(":")
	SyntaxErr      = []byte("-ERR syntax error\r\n")
	NoKeyErr       = []byte("-ERR no such key\r\n")
	OutOfRangeErr  = []byte("-ERR index out of range\r\n")
	SameObjectErr  = []byte("-ERR source and destination objects are the same\r\n")
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
	ProtocolWriter
	ProtocolReader

	PubSub

	DB() DB
	Server() Server
	SelectDB(id int) bool

	LookupKeyReadOrReply(key []byte, reply []byte) interface{}
	LookupKeyWriteOrReply(key []byte, reply []byte) interface{}

	BlockForKeys(keys [][]byte, timeout time.Duration)
}

type Writer interface {
	Write(b []byte)
	Flush()
}

type ProtocolWriter interface {
	AddReply(x []byte)
	AddReplyInt64(x int64)
	AddReplyFloat64(x float64)
	AddReplyMultiBulkLen(x int)
	AddReplyBulk(x []byte)
	AddReplyError(msg string)
	AddReplyStatus(msg string)
}

type ProtocolReader interface {
	ReadInlineCommand(line []byte) (CommandArg, error)
	ReadMultiBulkCommand(scanner *bufio.Scanner) (CommandArg, error)
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
