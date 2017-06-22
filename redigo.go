package redigo

import (
	"log"
	"net"
	"time"
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

const (
	REDIS_DEBUG = iota
	REDIS_VERBOSE
	REDIS_NOTICE
	REDIS_WARNING
	REDIS_LOG_RAW = 1 << 10
)

var Verbosity = REDIS_DEBUG

func RedigoLog(level int, fm string, objs ...interface{}) {
	if level >= Verbosity {
		if level&REDIS_LOG_RAW > 0 {
			flags := log.Flags()
			log.SetFlags(0)
			log.Printf(fm+"\n", objs...)
			log.SetFlags(flags)
		} else {
			log.Printf(fm+"\n", objs...)
		}
	}
}

type Writer interface {
	AddReply(x []byte)
	AddReplyByte(x byte)
	AddReplyString(x string)
	AddReplyInt64(x int64)
	AddReplyFloat64(x float64)
	AddReplyMultiBulkLen(x int)
	AddReplyBulk(x []byte)
	AddReplyError(msg string)
	AddReplyStatus(msg string)
	Flush() error
}

type Reader interface {
	Read() (*CommandArg, error)
}

type Executor interface {
	ProcessCommand(arg *CommandArg)
}

type Connection interface {
	Writer

	GetAddr() net.Addr
	SetBlockTimeout(t time.Duration)
	GetBlockedKeys() map[string]struct{}
	Close() error
	NextCommand(exec Executor) error
	Block()
	Unblock()
}

type Listener interface {
	Listen() chan Connection
	Count() int
	Close() error
}

type CommandArg struct {
	*Client
	Argv [][]byte
	Argc int
}

const (
	REDIS_CMD_WRITE = 1 << iota
	REDIS_CMD_READONLY
	REDIS_CMD_DENYOOM
	REDIS_CMD_ADMIN
	REDIS_CMD_PUBSUB
	REDIS_CMD_NOSCRIPT
	REDIS_CMD_RANDOM
	REDIS_CMD_SORT_FOR_SCRIPT
	REDIS_CMD_LOADING
	REDIS_CMD_STALE
	REDIS_CMD_SKIP_MONITOR
	REDIS_CMD_ASKING
	REDIS_CMD_FAST
)

/* Our command table.
 *
 * Every entry is composed of the following fields:
 *
 * name: a string representing the command name.
 * function: pointer to the C function implementing the command.
 * arity: number of arguments, it is possible to use -N to say >= N
 * sflags: command flags as string. See below for a table of flags.
 * flags: flags as bitmask. Computed by Redis using the 'sflags' field.
 * get_keys_proc: an optional function to get key arguments from a command.
 *                This is only used when the following three fields are not
 *                enough to specify what arguments are keys.
 * first_key_index: first argument that is a key
 * last_key_index: last argument that is a key
 * key_step: step to get all the keys from first to last argument. For instance
 *           in MSET the step is two since arguments are key,val,key,val,...
 * microseconds: microseconds of total execution time for this command.
 * calls: total number of calls of this command.
 *
 * The flags, microseconds and calls fields are computed by Redis and should
 * always be set to zero.
 *
 * Command flags are expressed using strings where every character represents
 * a flag. Later the populateCommandTable() function will take care of
 * populating the real 'flags' field using this characters.
 *
 * This is the meaning of the flags:
 *
 * w: write command (may modify the key space).
 * r: read command  (will never modify the key space).
 * m: may increase memory usage once called. Don't allow if out of memory.
 * a: admin command, like SAVE or SHUTDOWN.
 * p: Pub/Sub related command.
 * f: force replication of this command, regardless of server.dirty.
 * s: command not allowed in scripts.
 * R: random command. Command is not deterministic, that is, the same command
 *    with the same arguments, with the same key space, may have different
 *    results. For instance SPOP and RANDOMKEY are two random commands.
 * S: Sort command output array if called from script, so that the output
 *    is deterministic.
 * l: Allow command while loading the database.
 * t: Allow command while a slave has stale data but is not allowed to
 *    server this data. Normally no command is accepted in this condition
 *    but just a few.
 * M: Do not automatically propagate the command on MONITOR.
 * k: Perform an implicit ASKING for this command, so the command will be
 *    accepted in cluster mode if the slot is marked as 'importing'.
 * F: Fast command: O(1) or O(log(N)) command that should never delay
 *    its execution as long as the kernel scheduler is giving us time.
 *    Note that commands that may trigger a DEL as a side effect (like SET)
 *    are not fast commands.
 */
type Command struct {
	Name         string
	Proc         func(c *CommandArg)
	Arity        int
	SFlags       string
	Flags        int
	Calls        int64
	MicroSeconds int64
}


