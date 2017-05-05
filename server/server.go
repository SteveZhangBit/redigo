package server

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/command"
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
	Commands   map[string]*RedigoCommand
	nextToProc chan redigo.CommandArg
	// DB
	DBNum int
	dbs   []*RedigoDB
	// DB persistence
	dirty int // changes to DB from the last save
	// Fields used only for stas
	keyspaceHits   int
	keyspaceMisses int
	// Status
	StatStartTime   time.Time
	StatNumCommands int
}

/* =================================server init methods ======================================= */

func NewServer() *RedigoServer {
	s := &RedigoServer{
		PID:        os.Getpid(),
		Port:       6379,
		BindAddr:   []string{""},
		newClient:  make(chan *RedigoClient, 1),
		delClient:  make(chan *RedigoClient, 1),
		Verbosity:  REDIS_DEBUG,
		nextToProc: make(chan redigo.CommandArg, 1),
		DBNum:      4,
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
	r.signalHandler()

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

		case c := <-r.nextToProc:
			r.processCommand(c)
		}
	}
}

func (r *RedigoServer) signalHandler() {
	// Add system interrupt listener
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		for {
			<-interrupt
			r.RedigoLog(REDIS_WARNING, "Received SIGINT scheduling shutdown...")
			if r.PrepareForShutdown() {
				os.Exit(0)
			}
			r.RedigoLog(REDIS_WARNING, "SIGTERM received but errors trying to shut down the server, check the logs for more information")
		}
	}()
}

func (r *RedigoServer) listen() {
	for _, ip := range r.BindAddr {
		addr := fmt.Sprintf("%s:%d", ip, r.Port)
		listener, err := net.Listen("tcp", addr)

		if err != nil {
			r.RedigoLog(REDIS_WARNING, "Creating Server TCP listening socket %s: %s", addr, err)
			continue
		}

		r.listeners = append(r.listeners, listener)
		go func(l net.Listener, addr string) {
			defer l.Close()

			for {
				if conn, err := l.Accept(); err != nil {
					r.RedigoLog(REDIS_WARNING, "Accepting Server TCP listening socket %s: %s", addr, err)
					break
				} else {
					// Create client
					c := NewClient()
					c.server = r
					c.conn = conn
					c.Init()
					r.newClient <- c

				}
			}
		}(listener, addr)
	}
}

/* ================================= logging methods ======================================= */

func (r *RedigoServer) RedigoLog(level int, fmt string, objs ...interface{}) {
	if level >= r.Verbosity {
		if level&REDIS_LOG_RAW > 0 {
			flags := log.Flags()
			log.SetFlags(0)
			log.Printf(fmt+"\n", objs...)
			log.SetFlags(flags)
		} else {
			log.Printf(fmt+"\n", objs...)
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
	/* Now lookup the command and check ASAP about trivial error conditions
	 * such as wrong arity, bad command name and so forth. */
	// r.RedigoLog(REDIS_DEBUG, "Processing command: %s", c.Argv)

	cmd, ok := r.Commands[strings.ToLower(c.Argv[0])]
	if !ok {
		c.AddReplyError(fmt.Sprintf("unknown command '%s'", c.Argv[0]))
		return true
	} else if (cmd.Arity > 0 && cmd.Arity != c.Argc) || (c.Argc < -cmd.Arity) {
		c.AddReplyError(fmt.Sprintf("wrong number of arguments for '%s' command", cmd.Name))
		return true
	}

	r.call(c, cmd)
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
