package redigo

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/SteveZhangBit/redigo/shared"
)

const (
	REDIS_MAX_INTSET_ENTRIES = 512
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
	Proc         func(c CommandArg)
	Arity        int
	SFlags       string
	Flags        int
	Calls        int64
	MicroSeconds int64
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

var RedigoCommandTable []*RedigoCommand = []*RedigoCommand{
	{"get", GETCommand, 2, "rF", 0, 0, 0},
	{"set", SETCommand, -3, "wm", 0, 0, 0},
	{"setnx", SETNXCommand, 3, "wmF", 0, 0, 0},
	{"setex", SETEXCommand, 4, "wm", 0, 0, 0},
	{"psetex", PSETEXCommand, 4, "wm", 0, 0, 0},
	{"append", APPENDCommand, 3, "wm", 0, 0, 0},
	{"strlen", STRLENCommand, 2, "rF", 0, 0, 0},
	{"del", DELCommand, -2, "w", 0, 0, 0},
	{"exists", EXISTSCommand, -2, "rF", 0, 0, 0},
	{"setbit", SETBITCommand, 4, "wm", 0, 0, 0},
	{"getbit", GETBITCommand, 3, "rF", 0, 0, 0},
	{"setrange", SETRANGECommand, 4, "wm", 0, 0, 0},
	{"getrange", GETRANGECommand, 4, "r", 0, 0, 0},
	{"substr", GETRANGECommand, 4, "r", 0, 0, 0},
	{"incr", INCRCommand, 2, "wmF", 0, 0, 0},
	{"decr", DECRCommand, 2, "wmF", 0, 0, 0},
	{"mget", MGETCommand, -2, "r", 0, 0, 0},
	{"rpush", RPUSHCommand, -3, "wmF", 0, 0, 0},
	{"lpush", LPUSHCommand, -3, "wmF", 0, 0, 0},
	{"rpushx", RPUSHXCommand, 3, "wmF", 0, 0, 0},
	{"lpushx", LPUSHXCommand, 3, "wmF", 0, 0, 0},
	{"linsert", LINSERTCommand, 5, "wm", 0, 0, 0},
	{"rpop", RPOPCommand, 2, "wF", 0, 0, 0},
	{"lpop", LPOPCommand, 2, "wF", 0, 0, 0},
	{"brpop", BRPOPCommand, -3, "ws", 0, 0, 0},
	{"brpoplpush", BRPOPLPUSHCommand, 4, "wms", 0, 0, 0},
	{"blpop", BLPOPCommand, -3, "ws", 0, 0, 0},
	{"llen", LLENCommand, 2, "rF", 0, 0, 0},
	{"lindex", LINDEXCommand, 3, "r", 0, 0, 0},
	{"lset", LSETCommand, 4, "wm", 0, 0, 0},
	{"lrange", LRANGECommand, 4, "r", 0, 0, 0},
	{"ltrim", LTRIMCommand, 4, "w", 0, 0, 0},
	{"lrem", LREMCommand, 4, "w", 0, 0, 0},
	{"rpoplpush", RPOPLPUSHCommand, 3, "wm", 0, 0, 0},
	{"sadd", SADDCommand, -3, "wmF", 0, 0, 0},
	{"srem", SREMCommand, -3, "wF", 0, 0, 0},
	{"smove", SMOVECommand, 4, "wF", 0, 0, 0},
	{"sismember", SISMEMBERCommand, 3, "rF", 0, 0, 0},
	{"scard", SCARDCommand, 2, "rF", 0, 0, 0},
	{"spop", SPOPCommand, 2, "wRsF", 0, 0, 0},
	{"srandmember", SRANDMEMBERCommand, -2, "rR", 0, 0, 0},
	{"sinter", SINTERCommand, -2, "rS", 0, 0, 0},
	{"sinterstore", SINTERSTORECommand, -3, "wm", 0, 0, 0},
	{"sunion", SUNIONCommand, -2, "rS", 0, 0, 0},
	{"sunionstore", SUNIONSTORECommand, -3, "wm", 0, 0, 0},
	{"sdiff", SDIFFCommand, -2, "rS", 0, 0, 0},
	{"sdiffstore", SDIFFSTORECommand, -3, "wm", 0, 0, 0},
	{"smembers", SINTERCommand, 2, "rS", 0, 0, 0},
	{"sscan", SSCANCommand, -3, "rR", 0, 0, 0},
	{"zadd", ZADDCommand, -4, "wmF", 0, 0, 0},
	{"zincrby", ZINCRBYCommand, 4, "wmF", 0, 0, 0},
	{"zrem", ZREMCommand, -3, "wF", 0, 0, 0},
	{"zremrangebyscore", ZREMRANGEBYSCORECommand, 4, "w", 0, 0, 0},
	{"zremrangebyrank", ZREMRANGEBYRANKCommand, 4, "w", 0, 0, 0},
	{"zremrangebylex", ZREMRANGEBYLEXCommand, 4, "w", 0, 0, 0},
	{"zunionstore", ZUNIONSTORECommand, -4, "wm", 0, 0, 0},
	{"zinterstore", ZINTERSTORECommand, -4, "wm", 0, 0, 0},
	{"zrange", ZRANGECommand, -4, "r", 0, 0, 0},
	{"zrangebyscore", ZRANGEBYSCORECommand, -4, "r", 0, 0, 0},
	{"zrevrangebyscore", ZREVRANGEBYSCORECommand, -4, "r", 0, 0, 0},
	{"zrangebylex", ZRANGEBYLEXCommand, -4, "r", 0, 0, 0},
	{"zrevrangebylex", ZREVRANGEBYLEXCommand, -4, "r", 0, 0, 0},
	{"zcount", ZCOUNTCommand, 4, "rF", 0, 0, 0},
	{"zlexcount", ZLEXCOUNTCommand, 4, "rF", 0, 0, 0},
	{"zrevrange", ZREVRANGECommand, -4, "r", 0, 0, 0},
	{"zcard", ZCARDCommand, 2, "rF", 0, 0, 0},
	{"zscore", ZSCORECommand, 3, "rF", 0, 0, 0},
	{"zrank", ZRANKCommand, 3, "rF", 0, 0, 0},
	{"zrevrank", ZREVRANKCommand, 3, "rF", 0, 0, 0},
	{"zscan", ZSCANCommand, -3, "rR", 0, 0, 0},
	{"hset", HSETCommand, 4, "wmF", 0, 0, 0},
	{"hsetnx", HSETNXCommand, 4, "wmF", 0, 0, 0},
	{"hget", HGETCommand, 3, "rF", 0, 0, 0},
	{"hmset", HMSETCommand, -4, "wm", 0, 0, 0},
	{"hmget", HMGETCommand, -3, "r", 0, 0, 0},
	{"hincrby", HINCRBYCommand, 4, "wmF", 0, 0, 0},
	{"hincrbyfloat", HINCRBYFLOATCommand, 4, "wmF", 0, 0, 0},
	{"hdel", HDELCommand, -3, "wF", 0, 0, 0},
	{"hlen", HLENCommand, 2, "rF", 0, 0, 0},
	{"hkeys", HKEYSCommand, 2, "rS", 0, 0, 0},
	{"hvals", HVALSCommand, 2, "rS", 0, 0, 0},
	{"hgetall", HGETALLCommand, 2, "r", 0, 0, 0},
	{"hexists", HEXISTSCommand, 3, "rF", 0, 0, 0},
	{"hscan", HSCANCommand, -3, "rR", 0, 0, 0},
	{"incrby", INCRBYCommand, 3, "wmF", 0, 0, 0},
	{"decrby", DECRBYCommand, 3, "wmF", 0, 0, 0},
	{"incrbyfloat", INCRBYFLOATCommand, 3, "wmF", 0, 0, 0},
	{"getset", GETSETCommand, 3, "wm", 0, 0, 0},
	{"mset", MSETCommand, -3, "wm", 0, 0, 0},
	{"msetnx", MSETNXCommand, -3, "wm", 0, 0, 0},
	{"randomkey", RANDOMKEYCommand, 1, "rR", 0, 0, 0},
	{"select", SELECTCommand, 2, "rlF", 0, 0, 0},
	{"move", MOVECommand, 3, "wF", 0, 0, 0},
	{"rename", RENAMECommand, 3, "w", 0, 0, 0},
	{"renamenx", RENAMENXCommand, 3, "wF", 0, 0, 0},
	{"expire", EXPIRECommand, 3, "wF", 0, 0, 0},
	{"expireat", EXPIREATCommand, 3, "wF", 0, 0, 0},
	{"pexpire", PEXPIRECommand, 3, "wF", 0, 0, 0},
	{"pexpireat", PEXPIREATCommand, 3, "wF", 0, 0, 0},
	{"keys", KEYSCommand, 2, "rS", 0, 0, 0},
	{"scan", SCANCommand, -2, "rR", 0, 0, 0},
	{"dbsize", DBSIZECommand, 1, "rF", 0, 0, 0},
	{"auth", AUTHCommand, 2, "rsltF", 0, 0, 0},
	{"ping", PINGCommand, -1, "rtF", 0, 0, 0},
	{"echo", ECHOCommand, 2, "rF", 0, 0, 0},
	{"save", SAVECommand, 1, "ars", 0, 0, 0},
	{"bgsave", BGSAVECommand, 1, "ar", 0, 0, 0},
	{"bgrewriteaof", BGREWRITEAOFCommand, 1, "ar", 0, 0, 0},
	{"shutdown", SHUTDOWNCommand, -1, "arlt", 0, 0, 0},
	{"lastsave", LASTSAVECommand, 1, "rRF", 0, 0, 0},
	{"type", TYPECommand, 2, "rF", 0, 0, 0},
	// {"multi", MULTICommand, 1, "rsF", 0, 0, 0},
	// {"exec", EXECCommand, 1, "sM", 0, 0, 0},
	// {"discard", DISCARDCommand, 1, "rsF", 0, 0, 0},
	// {"sync", SYNCCommand, 1, "ars", 0, 0, 0},
	// {"psync", SYNCCommand, 3, "ars", 0, 0, 0},
	// {"replconf", REPLCONFCommand, -1, "arslt", 0, 0, 0},
	{"flushdb", FLUSHDBCommand, 1, "w", 0, 0, 0},
	{"flushall", FLUSHALLCommand, 1, "w", 0, 0, 0},
	// {"sort", SORTCommand, -2, "wm", 0, 0, 0},
	// {"info", INFOCommand, -1, "rlt", 0, 0, 0},
	// {"monitor", MONITORCommand, 1, "ars", 0, 0, 0},
	{"ttl", TTLCommand, 2, "rF", 0, 0, 0},
	{"pttl", PTTLCommand, 2, "rF", 0, 0, 0},
	{"persist", PERSISTCommand, 2, "wF", 0, 0, 0},
	// {"slaveof", SLAVEOFCommand, 3, "ast", 0, 0, 0},
	// {"role", ROLECommand, 1, "lst", 0, 0, 0},
	// {"debug", DEBUGCommand, -2, "as", 0, 0, 0},
	{"config", CONFIGCommand, -2, "art", 0, 0, 0},
	{"subscribe", SUBSCRIBECommand, -2, "rpslt", 0, 0, 0},
	{"unsubscribe", UNSUBSCRIBECommand, -1, "rpslt", 0, 0, 0},
	{"psubscribe", PSUBSCRIBECommand, -2, "rpslt", 0, 0, 0},
	{"punsubscribe", PUNSUBSCRIBECommand, -1, "rpslt", 0, 0, 0},
	{"publish", PUBLISHCommand, 3, "pltrF", 0, 0, 0},
	{"pubsub", PUBSUBCommand, -2, "pltrR", 0, 0, 0},
	// {"watch", WATCHCommand, -2, "rsF", 0, 0, 0},
	// {"unwatch", UNWATCHCommand, 1, "rsF", 0, 0, 0},
	// {"cluster", CLUSTERCommand, -2, "ar", 0, 0, 0},
	// {"restore", RESTORECommand, -4, "wm", 0, 0, 0},
	// {"restore-asking", RESTORECommand, -4, "wmk", 0, 0, 0},
	// {"migrate", MIGRATECommand, -6, "w", 0, 0, 0},
	// {"asking", ASKINGCommand, 1, "r", 0, 0, 0},
	// {"readonly", READONLYCommand, 1, "rF", 0, 0, 0},
	// {"readwrite", READWRITECommand, 1, "rF", 0, 0, 0},
	// {"dump", DUMPCommand, 2, "r", 0, 0, 0},
	// {"object", OBJECTCommand, 3, "r", 0, 0, 0},
	{"client", CLIENTCommand, -2, "rs", 0, 0, 0},
	// {"eval", EVALCommand, -3, "s", 0, 0, 0},
	// {"evalsha", EVALSHACommand, -3, "s", 0, 0, 0},
	// {"slowlog", SLOWLOGCommand, -2, "r", 0, 0, 0},
	// {"script", SCRIPTCommand, -2, "rs", 0, 0, 0},
	{"time", TIMECommand, 1, "rRF", 0, 0, 0},
	{"bitop", BITOPCommand, -4, "wm", 0, 0, 0},
	{"bitcount", BITCOUNTCommand, -2, "r", 0, 0, 0},
	{"bitpos", BITPOSCommand, -3, "r", 0, 0, 0},
	// {"wait", WAITCommand, 3, "rs", 0, 0, 0},
	{"command", COMMANDCommand, 0, "rlt", 0, 0, 0},
	// {"pfselftest", PFSELFTESTCommand, 1, "r", 0, 0, 0},
	// {"pfadd", PFADDCommand, -2, "wmF", 0, 0, 0},
	// {"pfcount", PFCOUNTCommand, -2, "r", 0, 0, 0},
	// {"pfmerge", PFMERGECommand, -2, "wm", 0, 0, 0},
	// {"pfdebug", PFDEBUGCommand, -3, "w", 0, 0, 0},
	// {"latency", LATENCYCommand, -2, "arslt", 0, 0, 0},
}

type RedigoServer struct {
	PID int
	// Networking
	Port      int
	BindAddr  []string
	Clients   *list.List
	listeners []net.Listener
	newClient chan *RedigoClient
	delClient chan *RedigoClient
	closed    chan bool
	// Logging
	Verbosity int
	// Command
	Command    map[string]*RedigoCommand
	nextToProc chan CommandArg
	// DB
	DBs   []*RedigoDB
	DBNum int
	// DB persistence
	Dirty int // changes to DB from the last save
	// Fields used only for stas
	KeyspaceHits   int
	KeyspaceMisses int
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
		closed:     make(chan bool),
		Verbosity:  REDIS_DEBUG,
		nextToProc: make(chan CommandArg, 1),
		DBNum:      4,
	}
	s.PopulateCommandTable()
	return s
}

// Populates the Redis Command Table starting from the hard coded list
func (r *RedigoServer) PopulateCommandTable() {
	r.Command = make(map[string]*RedigoCommand)
	for _, cmd := range RedigoCommandTable {
		r.Command[cmd.Name] = cmd

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

func (r *RedigoServer) Init() {
	r.Clients = list.New()
	r.Clients.Init()

	// Open the TCP listening socket for the user commands.
	r.Listen()
	// Abort if there are no listening sockets at all.
	if len(r.listeners) == 0 {
		r.RedigoLog(REDIS_WARNING, "Configured to not listen anywhere, exiting.")
		os.Exit(1)
	}

	// Create the Redis databases, and initialize other internal state.
	r.DBs = make([]*RedigoDB, r.DBNum)
	for i := 0; i < r.DBNum; i++ {
		r.DBs[i] = NewDB()
		r.DBs[i].ID = i
		r.DBs[i].Server = r
	}

	// A few stats we don't want to reset: server startup time, and peak mem.
	r.StatStartTime = time.Now()

	// Waiting to process commands, add clients or remove closed clients
	for {
		select {
		case c := <-r.newClient:
			r.RedigoLog(REDIS_DEBUG, "New connection on %s", c.Conn.RemoteAddr())
			r.Clients.PushBack(c)

		case c := <-r.delClient:
			for e := r.Clients.Front(); e != nil; e = e.Next() {
				if e.Value == c {
					r.RedigoLog(REDIS_DEBUG, "Remove closed client on %s", c.Conn.RemoteAddr())
					r.Clients.Remove(e)
					break
				}
			}

		case c := <-r.nextToProc:
			r.ProcessCommand(c)

		case x := <-r.closed:
			if x {
				break
			}
		}
	}
}

func (r *RedigoServer) Listen() {
	var wg sync.WaitGroup

	for _, ip := range r.BindAddr {
		addr := fmt.Sprintf("%s:%d", ip, r.Port)
		listener, err := net.Listen("tcp", addr)

		if err != nil {
			r.RedigoLog(REDIS_WARNING, "Creating Server TCP listening socket %s: %s", addr, err)
			continue
		}

		r.listeners = append(r.listeners, listener)
		wg.Add(1)
		go func(l net.Listener, addr string) {
			defer l.Close()
			defer wg.Done()

			for {
				if conn, err := l.Accept(); err != nil {
					r.RedigoLog(REDIS_WARNING, "Accepting Server TCP listening socket %s: %s", addr, err)
					break
				} else {
					// Create client
					c := NewClient()
					c.Server = r
					c.Conn = conn
					c.Init()
					r.newClient <- c
				}
			}
		}(listener, addr)
	}

	// If all the listeners have closed, close all channels. And the server will shutdown
	go func() {
		wg.Wait()
		r.closed <- true
	}()
}

/* ================================= logging methods ======================================= */

const (
	REDIS_DEBUG = iota
	REDIS_VERBOSE
	REDIS_NOTICE
	REDIS_WARNING
	REDIS_LOG_RAW = 1 << 10
)

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
func (r *RedigoServer) ProcessCommand(c CommandArg) bool {
	/* Now lookup the command and check ASAP about trivial error conditions
	 * such as wrong arity, bad command name and so forth. */
	// r.RedigoLog(REDIS_DEBUG, "Processing command: %s", c.Argv)

	cmd, ok := r.Command[strings.ToLower(c.Argv[0])]
	if !ok {
		c.AddReplyError(fmt.Sprintf("unknown command '%s'", c.Argv[0]))
		return true
	} else if (cmd.Arity > 0 && cmd.Arity != c.Argc) || (c.Argc < -cmd.Arity) {
		c.AddReplyError(fmt.Sprintf("wrong number of arguments for '%s' command", cmd.Name))
		return true
	}

	r.Call(c, cmd)
	return true
}

func (r *RedigoServer) Call(c CommandArg, cmd *RedigoCommand) {
	/* Call the command. */
	dirty := r.Dirty
	start := time.Now()
	cmd.Proc(c)
	duration := time.Now().Sub(start)
	dirty = r.Dirty - dirty
	if dirty < 0 {
		dirty = 0
	}

	cmd.MicroSeconds += int64(duration / time.Microsecond)
	cmd.Calls++

	r.StatNumCommands++
}

/*=========================================== Shutdown ======================================== */

func (r *RedigoServer) CloseListeningSockets() {

}

func (r *RedigoServer) PrepareForShutdown() {

}

/*================================= Server Side Commands ===================================== */

func AUTHCommand(c CommandArg) {

}

func PINGCommand(c CommandArg) {
	if c.Argc > 2 {
		c.AddReplyError(fmt.Sprintf("wrong number of arguments for '%s' command", c.Argv[0]))
		return
	}

	if c.Argc == 1 {
		c.AddReply(shared.Pong)
	} else {
		c.AddReplyBulk(c.Argv[1])
	}
}

func ECHOCommand(c CommandArg) {

}

func TIMECommand(c CommandArg) {

}

func ADDREPLYCommand(c CommandArg) {

}

func COMMANDCommand(c CommandArg) {

}
