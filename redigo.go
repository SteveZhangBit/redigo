package redigo

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
	EmptyMultiBulk = "*0\r\n"
	Pong           = "+PONG\r\n"
	WrongTypeErr   = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	Colon          = ":"
	SyntaxErr      = "-ERR syntax error\r\n"
	NoKeyErr       = "-ERR no such key\r\n"
	OutOfRangeErr  = "-ERR index out of range\r\n"
)

const (
	REDIS_NOTIFY_STRING = iota
	REDIS_NOTIFY_LIST
	REDIS_NOTIFY_HASH
	REDIS_NOTIFY_SET
	REDIS_NOTIFY_ZSET
	REDIS_NOTIFY_GENERIC
)

type Client interface {
	ClientReplyer
	PubSub

	DB() DB
	Server() Server
	Init()
	Close()
	IsClosed() bool

	LookupKeyReadOrReply(key string, reply string) interface{}
	LookupKeyWriteOrReply(key string, reply string) interface{}
}

type ClientReplyer interface {
	AddReply(x string)
	AddReplyInt64(x int64)
	AddReplyFloat64(x float64)
	AddReplyMultiBulkLen(x int)
	AddReplyBulk(x string)
	AddReplyError(msg string)
	AddReplyStatus(msg string)
}

type Server interface {
	Init()
	PrepareForShutdown() bool
	AddDirty(i int)
	RedigoLog(level int, fmt string, objs ...interface{})
}

type DB interface {
	GetID() int

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
	SignalListAsReady(key string)
}

type PubSub interface {
	NotifyKeyspaceEvent(t int, event string, key string, dbid int)
}

type CommandArg struct {
	Client
	Argv []string
	Argc int
}
