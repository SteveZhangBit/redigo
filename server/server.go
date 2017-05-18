package server

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/command"
	"github.com/SteveZhangBit/redigo/rtype"
)

const (
	REDIS_MAX_INTSET_ENTRIES = 512
)

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

const (
	REDIS_DEBUG = iota
	REDIS_VERBOSE
	REDIS_NOTICE
	REDIS_WARNING
	REDIS_LOG_RAW = 1 << 10
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
type RedigoCommand struct {
	Name         string
	Proc         func(c redigo.CommandArg)
	Arity        int
	SFlags       string
	Flags        int
	Calls        int64
	MicroSeconds int64
}

var RedigoCommandTable []*RedigoCommand = []*RedigoCommand{
	{"get", command.GETCommand, 2, "rF", 0, 0, 0},
	{"set", command.SETCommand, -3, "wm", 0, 0, 0},
	{"setnx", command.SETNXCommand, 3, "wmF", 0, 0, 0},
	{"setex", command.SETEXCommand, 4, "wm", 0, 0, 0},
	{"psetex", command.PSETEXCommand, 4, "wm", 0, 0, 0},
	{"append", command.APPENDCommand, 3, "wm", 0, 0, 0},
	{"strlen", command.STRLENCommand, 2, "rF", 0, 0, 0},
	{"del", command.DELCommand, -2, "w", 0, 0, 0},
	{"exists", command.EXISTSCommand, -2, "rF", 0, 0, 0},
	{"setbit", command.SETBITCommand, 4, "wm", 0, 0, 0},
	{"getbit", command.GETBITCommand, 3, "rF", 0, 0, 0},
	{"setrange", command.SETRANGECommand, 4, "wm", 0, 0, 0},
	{"getrange", command.GETRANGECommand, 4, "r", 0, 0, 0},
	{"substr", command.GETRANGECommand, 4, "r", 0, 0, 0},
	{"incr", command.INCRCommand, 2, "wmF", 0, 0, 0},
	{"decr", command.DECRCommand, 2, "wmF", 0, 0, 0},
	{"mget", command.MGETCommand, -2, "r", 0, 0, 0},
	{"rpush", command.RPUSHCommand, -3, "wmF", 0, 0, 0},
	{"lpush", command.LPUSHCommand, -3, "wmF", 0, 0, 0},
	{"rpushx", command.RPUSHXCommand, 3, "wmF", 0, 0, 0},
	{"lpushx", command.LPUSHXCommand, 3, "wmF", 0, 0, 0},
	{"linsert", command.LINSERTCommand, 5, "wm", 0, 0, 0},
	{"rpop", command.RPOPCommand, 2, "wF", 0, 0, 0},
	{"lpop", command.LPOPCommand, 2, "wF", 0, 0, 0},
	{"brpop", command.BRPOPCommand, -3, "ws", 0, 0, 0},
	{"brpoplpush", command.BRPOPLPUSHCommand, 4, "wms", 0, 0, 0},
	{"blpop", command.BLPOPCommand, -3, "ws", 0, 0, 0},
	{"llen", command.LLENCommand, 2, "rF", 0, 0, 0},
	{"lindex", command.LINDEXCommand, 3, "r", 0, 0, 0},
	{"lset", command.LSETCommand, 4, "wm", 0, 0, 0},
	{"lrange", command.LRANGECommand, 4, "r", 0, 0, 0},
	{"ltrim", command.LTRIMCommand, 4, "w", 0, 0, 0},
	{"lrem", command.LREMCommand, 4, "w", 0, 0, 0},
	{"rpoplpush", command.RPOPLPUSHCommand, 3, "wm", 0, 0, 0},
	{"sadd", command.SADDCommand, -3, "wmF", 0, 0, 0},
	{"srem", command.SREMCommand, -3, "wF", 0, 0, 0},
	{"smove", command.SMOVECommand, 4, "wF", 0, 0, 0},
	{"sismember", command.SISMEMBERCommand, 3, "rF", 0, 0, 0},
	{"scard", command.SCARDCommand, 2, "rF", 0, 0, 0},
	{"spop", command.SPOPCommand, 2, "wRsF", 0, 0, 0},
	{"srandmember", command.SRANDMEMBERCommand, -2, "rR", 0, 0, 0},
	{"sinter", command.SINTERCommand, -2, "rS", 0, 0, 0},
	{"sinterstore", command.SINTERSTORECommand, -3, "wm", 0, 0, 0},
	{"sunion", command.SUNIONCommand, -2, "rS", 0, 0, 0},
	{"sunionstore", command.SUNIONSTORECommand, -3, "wm", 0, 0, 0},
	{"sdiff", command.SDIFFCommand, -2, "rS", 0, 0, 0},
	{"sdiffstore", command.SDIFFSTORECommand, -3, "wm", 0, 0, 0},
	{"smembers", command.SINTERCommand, 2, "rS", 0, 0, 0},
	{"sscan", command.SSCANCommand, -3, "rR", 0, 0, 0},
	{"zadd", command.ZADDCommand, -4, "wmF", 0, 0, 0},
	{"zincrby", command.ZINCRBYCommand, 4, "wmF", 0, 0, 0},
	{"zrem", command.ZREMCommand, -3, "wF", 0, 0, 0},
	{"zremrangebyscore", command.ZREMRANGEBYSCORECommand, 4, "w", 0, 0, 0},
	{"zremrangebyrank", command.ZREMRANGEBYRANKCommand, 4, "w", 0, 0, 0},
	{"zremrangebylex", command.ZREMRANGEBYLEXCommand, 4, "w", 0, 0, 0},
	{"zunionstore", command.ZUNIONSTORECommand, -4, "wm", 0, 0, 0},
	{"zinterstore", command.ZINTERSTORECommand, -4, "wm", 0, 0, 0},
	{"zrange", command.ZRANGECommand, -4, "r", 0, 0, 0},
	{"zrangebyscore", command.ZRANGEBYSCORECommand, -4, "r", 0, 0, 0},
	{"zrevrangebyscore", command.ZREVRANGEBYSCORECommand, -4, "r", 0, 0, 0},
	{"zrangebylex", command.ZRANGEBYLEXCommand, -4, "r", 0, 0, 0},
	{"zrevrangebylex", command.ZREVRANGEBYLEXCommand, -4, "r", 0, 0, 0},
	{"zcount", command.ZCOUNTCommand, 4, "rF", 0, 0, 0},
	{"zlexcount", command.ZLEXCOUNTCommand, 4, "rF", 0, 0, 0},
	{"zrevrange", command.ZREVRANGECommand, -4, "r", 0, 0, 0},
	{"zcard", command.ZCARDCommand, 2, "rF", 0, 0, 0},
	{"zscore", command.ZSCORECommand, 3, "rF", 0, 0, 0},
	{"zrank", command.ZRANKCommand, 3, "rF", 0, 0, 0},
	{"zrevrank", command.ZREVRANKCommand, 3, "rF", 0, 0, 0},
	{"zscan", command.ZSCANCommand, -3, "rR", 0, 0, 0},
	{"hset", command.HSETCommand, 4, "wmF", 0, 0, 0},
	{"hsetnx", command.HSETNXCommand, 4, "wmF", 0, 0, 0},
	{"hget", command.HGETCommand, 3, "rF", 0, 0, 0},
	{"hmset", command.HMSETCommand, -4, "wm", 0, 0, 0},
	{"hmget", command.HMGETCommand, -3, "r", 0, 0, 0},
	{"hincrby", command.HINCRBYCommand, 4, "wmF", 0, 0, 0},
	{"hincrbyfloat", command.HINCRBYFLOATCommand, 4, "wmF", 0, 0, 0},
	{"hdel", command.HDELCommand, -3, "wF", 0, 0, 0},
	{"hlen", command.HLENCommand, 2, "rF", 0, 0, 0},
	{"hkeys", command.HKEYSCommand, 2, "rS", 0, 0, 0},
	{"hvals", command.HVALSCommand, 2, "rS", 0, 0, 0},
	{"hgetall", command.HGETALLCommand, 2, "r", 0, 0, 0},
	{"hexists", command.HEXISTSCommand, 3, "rF", 0, 0, 0},
	{"hscan", command.HSCANCommand, -3, "rR", 0, 0, 0},
	{"incrby", command.INCRBYCommand, 3, "wmF", 0, 0, 0},
	{"decrby", command.DECRBYCommand, 3, "wmF", 0, 0, 0},
	{"incrbyfloat", command.INCRBYFLOATCommand, 3, "wmF", 0, 0, 0},
	{"getset", command.GETSETCommand, 3, "wm", 0, 0, 0},
	{"mset", command.MSETCommand, -3, "wm", 0, 0, 0},
	{"msetnx", command.MSETNXCommand, -3, "wm", 0, 0, 0},
	{"randomkey", command.RANDOMKEYCommand, 1, "rR", 0, 0, 0},
	{"select", command.SELECTCommand, 2, "rlF", 0, 0, 0},
	{"move", command.MOVECommand, 3, "wF", 0, 0, 0},
	{"rename", command.RENAMECommand, 3, "w", 0, 0, 0},
	{"renamenx", command.RENAMENXCommand, 3, "wF", 0, 0, 0},
	{"expire", command.EXPIRECommand, 3, "wF", 0, 0, 0},
	{"expireat", command.EXPIREATCommand, 3, "wF", 0, 0, 0},
	{"pexpire", command.PEXPIRECommand, 3, "wF", 0, 0, 0},
	{"pexpireat", command.PEXPIREATCommand, 3, "wF", 0, 0, 0},
	{"keys", command.KEYSCommand, 2, "rS", 0, 0, 0},
	{"scan", command.SCANCommand, -2, "rR", 0, 0, 0},
	{"dbsize", command.DBSIZECommand, 1, "rF", 0, 0, 0},
	{"auth", command.AUTHCommand, 2, "rsltF", 0, 0, 0},
	{"ping", command.PINGCommand, -1, "rtF", 0, 0, 0},
	{"echo", command.ECHOCommand, 2, "rF", 0, 0, 0},
	{"save", command.SAVECommand, 1, "ars", 0, 0, 0},
	{"bgsave", command.BGSAVECommand, 1, "ar", 0, 0, 0},
	{"bgrewriteaof", command.BGREWRITEAOFCommand, 1, "ar", 0, 0, 0},
	{"shutdown", command.SHUTDOWNCommand, -1, "arlt", 0, 0, 0},
	{"lastsave", command.LASTSAVECommand, 1, "rRF", 0, 0, 0},
	{"type", command.TYPECommand, 2, "rF", 0, 0, 0},
	// {"multi", command.MULTICommand, 1, "rsF", 0, 0, 0},
	// {"exec", command.EXECCommand, 1, "sM", 0, 0, 0},
	// {"discard", command.DISCARDCommand, 1, "rsF", 0, 0, 0},
	// {"sync", command.SYNCCommand, 1, "ars", 0, 0, 0},
	// {"psync", command.SYNCCommand, 3, "ars", 0, 0, 0},
	// {"replconf", command.REPLCONFCommand, -1, "arslt", 0, 0, 0},
	{"flushdb", command.FLUSHDBCommand, 1, "w", 0, 0, 0},
	{"flushall", command.FLUSHALLCommand, 1, "w", 0, 0, 0},
	// {"sort", command.SORTCommand, -2, "wm", 0, 0, 0},
	// {"info", command.INFOCommand, -1, "rlt", 0, 0, 0},
	// {"monitor", command.MONITORCommand, 1, "ars", 0, 0, 0},
	{"ttl", command.TTLCommand, 2, "rF", 0, 0, 0},
	{"pttl", command.PTTLCommand, 2, "rF", 0, 0, 0},
	{"persist", command.PERSISTCommand, 2, "wF", 0, 0, 0},
	// {"slaveof", command.SLAVEOFCommand, 3, "ast", 0, 0, 0},
	// {"role", command.ROLECommand, 1, "lst", 0, 0, 0},
	// {"debug", command.DEBUGCommand, -2, "as", 0, 0, 0},
	{"config", command.CONFIGCommand, -2, "art", 0, 0, 0},
	{"subscribe", command.SUBSCRIBECommand, -2, "rpslt", 0, 0, 0},
	{"unsubscribe", command.UNSUBSCRIBECommand, -1, "rpslt", 0, 0, 0},
	{"psubscribe", command.PSUBSCRIBECommand, -2, "rpslt", 0, 0, 0},
	{"punsubscribe", command.PUNSUBSCRIBECommand, -1, "rpslt", 0, 0, 0},
	{"publish", command.PUBLISHCommand, 3, "pltrF", 0, 0, 0},
	{"pubsub", command.PUBSUBCommand, -2, "pltrR", 0, 0, 0},
	// {"watch", command.WATCHCommand, -2, "rsF", 0, 0, 0},
	// {"unwatch", command.UNWATCHCommand, 1, "rsF", 0, 0, 0},
	// {"cluster", command.CLUSTERCommand, -2, "ar", 0, 0, 0},
	// {"restore", command.RESTORECommand, -4, "wm", 0, 0, 0},
	// {"restore-asking", command.RESTORECommand, -4, "wmk", 0, 0, 0},
	// {"migrate", command.MIGRATECommand, -6, "w", 0, 0, 0},
	// {"asking", command.ASKINGCommand, 1, "r", 0, 0, 0},
	// {"readonly", command.READONLYCommand, 1, "rF", 0, 0, 0},
	// {"readwrite", command.READWRITECommand, 1, "rF", 0, 0, 0},
	// {"dump", command.DUMPCommand, 2, "r", 0, 0, 0},
	// {"object", command.OBJECTCommand, 3, "r", 0, 0, 0},
	{"client", command.CLIENTCommand, -2, "rs", 0, 0, 0},
	// {"eval", command.EVALCommand, -3, "s", 0, 0, 0},
	// {"evalsha", command.EVALSHACommand, -3, "s", 0, 0, 0},
	// {"slowlog", command.SLOWLOGCommand, -2, "r", 0, 0, 0},
	// {"script", command.SCRIPTCommand, -2, "rs", 0, 0, 0},
	{"time", command.TIMECommand, 1, "rRF", 0, 0, 0},
	{"bitop", command.BITOPCommand, -4, "wm", 0, 0, 0},
	{"bitcount", command.BITCOUNTCommand, -2, "r", 0, 0, 0},
	{"bitpos", command.BITPOSCommand, -3, "r", 0, 0, 0},
	// {"wait", command.WAITCommand, 3, "rs", 0, 0, 0},
	{"command", command.COMMANDCommand, 0, "rlt", 0, 0, 0},
	// {"pfselftest", command.PFSELFTESTCommand, 1, "r", 0, 0, 0},
	// {"pfadd", command.PFADDCommand, -2, "wmF", 0, 0, 0},
	// {"pfcount", command.PFCOUNTCommand, -2, "r", 0, 0, 0},
	// {"pfmerge", command.PFMERGECommand, -2, "wm", 0, 0, 0},
	// {"pfdebug", command.PFDEBUGCommand, -3, "w", 0, 0, 0},
	// {"latency", command.LATENCYCommand, -2, "arslt", 0, 0, 0},
}

type RedigoServer struct {
	PID int
	// Networking
	Port      int
	BindAddr  []string
	clients   *list.List
	listeners []net.Listener
	newClient chan *RedigoClient
	delClient chan *RedigoClient
	// Logging
	Verbosity int
	// Command
	Commands map[string]*RedigoCommand
	procLock sync.Mutex
	// DB
	DBNum int
	dbs   []*RedigoDB
	// DB persistence
	dirty          int // changes to DB from the last save
	keyspaceMisses int
	keyspaceHits   int
	// Status
	StatStartTime   time.Time
	StatNumCommands int
	// Blocked clients
	blockedClients int
	readyKeys      []ReadyKey
}

/* The following structure represents a node in the server.ready_keys list,
 * where we accumulate all the keys that had clients blocked with a blocking
 * operation such as B[LR]POP, but received new data in the context of the
 * last executed command.
 *
 * After the execution of every command or script, we run this list to check
 * if as a result we should serve data to clients blocked, unblocking them.
 * Note that server.ready_keys will not have duplicates as there dictionary
 * also called ready_keys in every structure representing a Redis database,
 * where we make sure to remember if a given key was already added in the
 * server.ready_keys list. */
type ReadyKey struct {
	DB  *RedigoDB
	Key []byte
}

/* =================================server init methods ======================================= */

func NewServer() *RedigoServer {
	s := &RedigoServer{
		PID:       os.Getpid(),
		Port:      6379,
		BindAddr:  []string{""},
		newClient: make(chan *RedigoClient, 1),
		delClient: make(chan *RedigoClient, 1),
		Verbosity: REDIS_WARNING,
		DBNum:     4,
	}
	s.clients = list.New()
	s.clients.Init()
	s.populateCommandTable()
	return s
}

// Populates the Redis Command Table starting from the hard coded list
func (r *RedigoServer) populateCommandTable() {
	r.Commands = make(map[string]*RedigoCommand)
	for _, cmd := range RedigoCommandTable {
		r.Commands[cmd.Name] = cmd

		for _, c := range cmd.SFlags {
			switch c {
			case 'w':
				cmd.Flags |= REDIS_CMD_WRITE
			case 'r':
				cmd.Flags |= REDIS_CMD_READONLY
			case 'm':
				cmd.Flags |= REDIS_CMD_DENYOOM
			case 'a':
				cmd.Flags |= REDIS_CMD_ADMIN
			case 'p':
				cmd.Flags |= REDIS_CMD_PUBSUB
			case 's':
				cmd.Flags |= REDIS_CMD_NOSCRIPT
			case 'R':
				cmd.Flags |= REDIS_CMD_RANDOM
			case 'S':
				cmd.Flags |= REDIS_CMD_SORT_FOR_SCRIPT
			case 'l':
				cmd.Flags |= REDIS_CMD_LOADING
			case 't':
				cmd.Flags |= REDIS_CMD_STALE
			case 'M':
				cmd.Flags |= REDIS_CMD_SKIP_MONITOR
			case 'k':
				cmd.Flags |= REDIS_CMD_ASKING
			case 'F':
				cmd.Flags |= REDIS_CMD_FAST
			default:
				panic("Unsupported command flag")
			}
		}
	}
}

func (r *RedigoServer) AddDirty(i int) {
	r.dirty += i
}

func (r *RedigoServer) Init() {
	// Open the TCP listening socket for the user commands.
	r.listen()
	// Abort if there are no listening sockets at all.
	if len(r.listeners) == 0 {
		r.RedigoLog(REDIS_WARNING, "Configured to not listen anywhere, exiting.")
		os.Exit(1)
	}

	// Create the Redis databases, and initialize other internal state.
	r.dbs = make([]*RedigoDB, r.DBNum)
	for i := 0; i < r.DBNum; i++ {
		db := NewDB()
		db.id = i
		db.server = r

		r.dbs[i] = db
	}

	// A few stats we don't want to reset: server startup time, and peak mem.
	r.StatStartTime = time.Now()

	// Add system interrupt listener
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	// Waiting to process commands, add clients or remove closed clients
	for {
		select {
		case c := <-r.newClient:
			r.RedigoLog(REDIS_DEBUG, "New connection on %s", c.conn.RemoteAddr())
			r.clients.PushBack(c)

		case c := <-r.delClient:
			for e := r.clients.Front(); e != nil; e = e.Next() {
				if e.Value == c {
					r.clients.Remove(e)
					break
				}
			}

		case <-interrupt:
			r.RedigoLog(REDIS_WARNING, "Received SIGINT scheduling shutdown...")
			if r.PrepareForShutdown() {
				return
			}
			r.RedigoLog(REDIS_WARNING, "SIGTERM received but errors trying to shut down the server, check the logs for more information")
		}
	}
}

func (r *RedigoServer) listen() {
	for _, ip := range r.BindAddr {
		addr := fmt.Sprintf("%s:%d", ip, r.Port)
		listener, err := net.Listen("tcp", addr)

		if err != nil {
			r.RedigoLog(REDIS_DEBUG, "Creating Server TCP listening socket %s: %s", addr, err)
			continue
		}

		r.listeners = append(r.listeners, listener)
		go func(l net.Listener, addr string) {
			defer l.Close()

			for {
				if conn, err := l.Accept(); err != nil {
					r.RedigoLog(REDIS_DEBUG, "Accepting Server TCP listening socket %s: %s", addr, err)
					break
				} else {
					// Create client
					c := NewClient()
					c.server = r
					c.conn = conn
					c.init()
					r.newClient <- c

				}
			}
		}(listener, addr)
	}
}

/* ================================= logging methods ======================================= */

func (r *RedigoServer) RedigoLog(level int, fm string, objs ...interface{}) {
	if level >= r.Verbosity {
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

/*====================================== Process command ======================================== */
/* If this function gets called we already read a whole
 * command, arguments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the
 * server for a bulk read from the client.
 *
 * If 1 is returned the client is still alive and valid and
 * other operations can be performed by the caller. Otherwise
 * if 0 is returned the client was destroyed (i.e. after QUIT). */
func (r *RedigoServer) processCommand(c redigo.CommandArg) bool {
	r.procLock.Lock()
	/* Now lookup the command and check ASAP about trivial error conditions
	 * such as wrong arity, bad command name and so forth. */

	cmd, ok := r.Commands[string(redigo.ToLower(c.Argv[0]))]
	c.Client.(*RedigoClient).lastcmd = cmd
	if !ok {
		c.AddReplyError(fmt.Sprintf("unknown command '%s'", string(c.Argv[0])))
		r.procLock.Unlock()
		c.Client.(*RedigoClient).Flush()
		return true
	} else if (cmd.Arity > 0 && cmd.Arity != c.Argc) || (c.Argc < -cmd.Arity) {
		c.AddReplyError(fmt.Sprintf("wrong number of arguments for '%s' command", cmd.Name))
		r.procLock.Unlock()
		c.Client.(*RedigoClient).Flush()
		return true
	}

	r.call(c, cmd)
	// If there are clients blocked on lists
	if len(r.readyKeys) > 0 {
		r.handleClientsBlockedOnLists()
	}
	r.procLock.Unlock()
	c.Client.(*RedigoClient).Flush()
	return true
}

func (r *RedigoServer) call(c redigo.CommandArg, cmd *RedigoCommand) {
	/* Call the command. */
	dirty := r.dirty
	start := time.Now()
	cmd.Proc(c)
	duration := time.Now().Sub(start)
	dirty = r.dirty - dirty
	if dirty < 0 {
		dirty = 0
	}

	cmd.MicroSeconds += int64(duration / time.Microsecond)
	cmd.Calls++

	r.StatNumCommands++
}

/* This function should be called by Redis every time a single command,
 * a MULTI/EXEC block, or a Lua script, terminated its execution after
 * being called by a client.
 *
 * All the keys with at least one client blocked that received at least
 * one new element via some PUSH operation are accumulated into
 * the server.ready_keys list. This function will run the list and will
 * serve clients accordingly. Note that the function will iterate again and
 * again as a result of serving BRPOPLPUSH we can have new blocking clients
 * to serve because of the PUSH side of BRPOPLPUSH. */
func (r *RedigoServer) handleClientsBlockedOnLists() {
	l := r.readyKeys
	for len(l) > 0 {
		rk := l[0]
		/* First of all remove this key from db->ready_keys so that
		 * we can safely call signalListAsReady() against this key. */
		delete(rk.DB.readyKeys, string(rk.Key))

		/* If the key exists and it's a list, serve blocked clients
		 * with data. */
		if o, ok := rk.DB.LookupKeyWrite(rk.Key).(rtype.List); ok {
			/* We serve clients in the same order they blocked for
			 * this key, from the first blocked to the last. */
			if cls, ok := rk.DB.blockingKeys[string(rk.Key)]; ok {
				for i := 0; i < len(cls); i++ {
					var where int
					var reciever *RedigoClient = cls[i]
					var val rtype.String

					if reciever.lastcmd != nil && reciever.lastcmd.Name == "blpop" {
						where = rtype.REDIS_LIST_HEAD
						val = o.PopFront().Value()
					} else {
						where = rtype.REDIS_LIST_TAIL
						val = o.PopBack().Value()
					}

					if val != nil {
						reciever.unblock(true)
						if !r.serveClientBlockedOnList(reciever, rk.Key, rk.DB, val, where) {
							/* If we failed serving the client we need
							 * to also undo the POP operation. */
							if where == rtype.REDIS_LIST_HEAD {
								o.PushFront(val)
							} else {
								o.PushBack(val)
							}
						}
					} else {
						// ???
						break
					}
				}
			}
			if o.Len() == 0 {
				rk.DB.Delete(rk.Key)
			}
			/* We don't call signalModifiedKey() as it was already called
			 * when an element was pushed on the list. */
		}
		l = append(l[:0], l[1:]...)
	}
}

/* This is a helper function for handleClientsBlockedOnLists(). It's work
 * is to serve a specific client (receiver) that is blocked on 'key'
 * in the context of the specified 'db', doing the following:
 *
 * 1) Provide the client with the 'value' element.
 * 2) If the dstkey is not NULL (we are serving a BRPOPLPUSH) also push the
 *    'value' element on the destination list (the LPUSH side of the command).
 * 3) Propagate the resulting BRPOP, BLPOP and additional LPUSH if any into
 *    the AOF and replication channel.
 *
 * The argument 'where' is REDIS_TAIL or REDIS_HEAD, and indicates if the
 * 'value' element was popped fron the head (BLPOP) or tail (BRPOP) so that
 * we can propagate the command properly.
 *
 * The function returns REDIS_OK if we are able to serve the client, otherwise
 * REDIS_ERR is returned to signal the caller that the list POP operation
 * should be undone as the client was not served: This only happens for
 * BRPOPLPUSH that fails to push the value to the destination key as it is
 * of the wrong type. */
func (r *RedigoServer) serveClientBlockedOnList(receiver *RedigoClient, key []byte, db *RedigoDB, val rtype.String, where int) bool {
	receiver.AddReplyMultiBulkLen(2)
	receiver.AddReplyBulk(key)
	receiver.AddReplyBulk(val.Bytes())
	return true
}

/*=========================================== Shutdown ======================================== */

func (r *RedigoServer) closeListeningSockets() {
	for _, l := range r.listeners {
		l.Close()
	}
}

func (r *RedigoServer) PrepareForShutdown() bool {
	r.RedigoLog(REDIS_WARNING, "User requested shutdown...")
	r.closeListeningSockets()
	r.RedigoLog(REDIS_WARNING, "%s is now ready to exit, bye bye...", "Redis")
	return true
}
