package redigo

import (
	"bufio"
	"time"
)

const (
	Version = "0.0.1"
)

const (
	CRLF           = "\r\n"
	OK             = "+OK\r\n"
	Err            = "-ERR\r\n"
	CZero          = ":0\r\n"
	COne           = ":1\r\n"
	CNegOne        = ":-1\r\n"
	NullBulk       = "$-1\r\n"
	NullMultiBulk  = "*-1\r\n"
	EmptyMultiBulk = "*0\r\n"
	Pong           = "+PONG\r\n"
	WrongTypeErr   = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	Colon          = ":"
	SyntaxErr      = "-ERR syntax error\r\n"
	NoKeyErr       = "-ERR no such key\r\n"
	OutOfRangeErr  = "-ERR index out of range\r\n"
	SameObjectErr  = "-ERR source and destination objects are the same\r\n"
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

	LookupKeyReadOrReply(key string, reply string) interface{}
	LookupKeyWriteOrReply(key string, reply string) interface{}

	BlockForKeys(keys []string, timeout time.Duration)
}

type ProtocolWriter interface {
	AddReply(x string)
	AddReplyInt64(x int64)
	AddReplyFloat64(x float64)
	AddReplyMultiBulkLen(x int)
	AddReplyBulk(x string)
	AddReplyError(msg string)
	AddReplyStatus(msg string)
}

type ProtocolReader interface {
	ReadInlineCommand(line string, c Client) (CommandArg, error)
	ReadMultiBulkCommand(scanner *bufio.Scanner, c Client) (CommandArg, error)
}

type Server interface {
	Init()
	PrepareForShutdown() bool
	AddDirty(i int)
	// RedigoLog(level int, fm string, objs ...interface{})
}

/* Redis database representation. There are multiple databases identified
 * by integers from 0 (the default database) up to the max configured
 * database. The database number is the 'id' field in the structure. */
type DB interface {
	GetID() int
	GetDict() map[string]interface{}

	LookupKey(key string) interface{}
	LookupKeyRead(key string) interface{}
	LookupKeyWrite(key string) interface{}

	Add(key string, val interface{})
	Update(key string, val interface{})
	Delete(key string) (ok bool)
	SetKeyPersist(key string, val interface{})
	Exists(key string) (ok bool)
	RandomKey() (key string)

	SignalModifyKey(key string)

	ExpireIfNeed(key string) bool
	GetExpire(key string) time.Duration
	SetExpire(key string, t time.Duration)
}

type PubSub interface {
	NotifyKeyspaceEvent(t int, event string, key string, dbid int)
}

type CommandArg struct {
	Client
	Argv []string
	Argc int
}
