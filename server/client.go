package server

import (
	"bufio"
	"net"
	"time"

	"github.com/SteveZhangBit/redigo"
)

// Client flags
const (
	REDIS_SLAVE              = 1 << iota // This client is a slave server
	REDIS_MASTER                         // This client is a master server
	REDIS_MONITOR                        // This client is a slave monitor, see MONITOR
	REDIS_MULTI                          // This client is in a MULTI context
	REDIS_BLOCKED                        // The client is waiting in a blocking operation
	REDIS_DIRTY_CAS                      // Watched keys modified. EXEC will fail.
	REDIS_CLOSE_AFTER_REPLY              // Close after writing entire reply.
	REDIS_UNBLOCKED                      // This client was unblocked and is stored in server.unblocked_clients
	REDIS_LUA_CLIENT                     // This is a non connected client used by Lua
	REDIS_ASKING                         // Client issued the ASKING command
	REDIS_CLOSE_ASAP                     // Close this client ASAP
	REDIS_UNIX_SOCKET                    // Client connected via Unix domain socket
	REDIS_DIRTY_EXEC                     // EXEC will fail for errors while queueing
	REDIS_MASTER_FORCE_REPLY             // Queue replies even if is master
	REDIS_FORCE_AOF                      // Force AOF propagation of current cmd.
	REDIS_FORCE_REPL                     // Force replication of current cmd.
	REDIS_PRE_PSYNC                      // Instance don't understand PSYNC.
	REDIS_READONLY                       // Cluster client is in read-only state.
	REDIS_PUBSUB                         // Client is in Pub/Sub mode.
)

type RedigoClient struct {
	*redigo.RESPWriter
	*redigo.RESPReader

	*RedigoPubSub

	Flags int

	db     *RedigoDB
	server *RedigoServer

	conn    net.Conn
	outbuf  chan string
	closed  chan struct{}
	cmddone chan struct{}
	lastcmd *RedigoCommand

	bpop    *ClientBlockState
	blocked chan struct{}
}

type ClientBlockState struct {
	Timeout time.Duration
	Keys    map[string]struct{}
}

func NewClient() *RedigoClient {
	c := &RedigoClient{
		outbuf:  make(chan string, 10),
		closed:  make(chan struct{}),
		cmddone: make(chan struct{}),
		bpop:    &ClientBlockState{Keys: make(map[string]struct{})},
		blocked: make(chan struct{}),
	}
	c.RESPWriter = redigo.NewRESPWriterFunc(func(x string) {
		if c.Flags&REDIS_CLOSE_AFTER_REPLY > 0 {
			return
		}
		c.outbuf <- x
	})

	return c
}

func (r *RedigoClient) DB() redigo.DB {
	return r.db
}

func (r *RedigoClient) Server() redigo.Server {
	return r.server
}

func (r *RedigoClient) init() {
	r.SelectDB(0)

	go r.readNextCommand()
	go r.sendReplyToClient()
}

func (r *RedigoClient) CommandDone() {
	r.cmddone <- struct{}{}
}

func (r *RedigoClient) SelectDB(id int) bool {
	if id < 0 || id > len(r.server.dbs) {
		return false
	} else {
		r.db = r.server.dbs[id]
		return true
	}
}

func (r *RedigoClient) sendReplyToClient() {
	for !r.isClosed() {
		select {
		case x := <-r.outbuf:
			if _, err := r.conn.Write([]byte(x)); err != nil {
				r.server.RedigoLog(REDIS_VERBOSE, "Error writing to client: %s", err)
				r.close()
			}
		default:
			if r.Flags&REDIS_CLOSE_AFTER_REPLY > 0 {
				r.close()
			}
		}

	}
}

func (r *RedigoClient) readNextCommand() {
	var line string
	var arg redigo.CommandArg
	var err error

	scanner := bufio.NewScanner(r.conn)
	for {
		// If the client is set to be blocked
		if r.Flags&REDIS_BLOCKED > 0 {
			if r.bpop.Timeout > 0 {
				select {
				case <-time.After(r.bpop.Timeout):
					r.AddReply(redigo.NullMultiBulk)
					r.unblock(false)
				case <-r.blocked:
				}
			} else {
				<-r.blocked
			}
		}

		if !scanner.Scan() {
			break
		}
		line = scanner.Text()
		if line == "" {
			continue
		}

		if line[0] != '*' {
			arg, err = r.ReadInlineCommand(line, r)
		} else {
			arg, err = r.ReadMultiBulkCommand(scanner, r)
		}

		if err != nil {
			r.AddReplyError(err.Error())
			r.setProtocolError()
		} else {
			r.server.nextToProc <- arg
			// Wait until the command done
			<-r.cmddone
		}
	}
	r.close()
}

func (r *RedigoClient) setProtocolError() {
	r.Flags |= REDIS_CLOSE_AFTER_REPLY
}

func (r *RedigoClient) close() {
	if !r.isClosed() {
		r.server.RedigoLog(REDIS_DEBUG, "Closing connection on: %s", r.conn.RemoteAddr())
		r.conn.Close()
		close(r.closed)
		r.server.delClient <- r
	}
}

func (r *RedigoClient) isClosed() bool {
	select {
	case <-r.closed:
		return true
	default:
		return false
	}
}

func (r *RedigoClient) LookupKeyReadOrReply(key string, reply string) interface{} {
	x := r.db.LookupKeyRead(key)
	if x == nil {
		r.AddReply(reply)
	}
	return x
}

func (r *RedigoClient) LookupKeyWriteOrReply(key string, reply string) interface{} {
	x := r.db.LookupKeyWrite(key)
	if x == nil {
		r.AddReply(reply)
	}
	return x
}

// Set a client in blocking mode for the specified key, with the specified timeout
func (r *RedigoClient) BlockForKeys(keys []string, timeout time.Duration) {
	r.bpop.Timeout = timeout

	for i := 0; i < len(keys); i++ {
		var cls []*RedigoClient
		var ok bool

		// If the key already exists in the dict ignore it
		if _, ok = r.bpop.Keys[keys[i]]; ok {
			continue
		}
		r.bpop.Keys[keys[i]] = struct{}{}
		if cls, ok = r.db.blockingKeys[keys[i]]; !ok {
			cls = make([]*RedigoClient, 0, 1)
		}
		r.db.blockingKeys[keys[i]] = append(cls, r)
	}
	r.block()
}

/* Block a client for the specific operation type. Once the REDIS_BLOCKED
 * flag is set client query buffer is not longer processed, but accumulated,
 * and will be processed when the client is unblocked. */
func (r *RedigoClient) block() {
	r.Flags |= REDIS_BLOCKED
	r.server.blockedClients++
}

/* Unblock a client calling the right function depending on the kind
 * of operation the client is blocking for. */
func (r *RedigoClient) unblock(signal bool) {
	r.unblockWaitingData()
	/* Clear the flags, and put the client in the unblocked list so that
	 * we'll process new commands in its query buffer ASAP. */
	r.Flags &= ^REDIS_BLOCKED
	r.Flags |= REDIS_UNBLOCKED
	r.server.blockedClients--
	if signal {
		r.blocked <- struct{}{}
	}
}

/* Unblock a client that's waiting in a blocking operation such as BLPOP.
 * You should never call this function directly, but unblockClient() instead. */
func (r *RedigoClient) unblockWaitingData() {
	for key := range r.bpop.Keys {
		var idx int
		var c *RedigoClient
		// Remove this client from the list of clients waiting for this key
		cls := r.db.blockingKeys[key]
		for idx, c = range cls {
			if c == r {
				break
			}
		}
		r.db.blockingKeys[key] = append(cls[:idx], cls[idx+1:]...)
		// If the list is empty we need to remove it to avoid wasting memory
		if len(cls) == 0 {
			delete(r.db.blockingKeys, key)
		}

		// Cleanup the client structure
		delete(r.bpop.Keys, key)
	}
}
