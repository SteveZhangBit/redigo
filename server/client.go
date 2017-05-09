package server

import (
	"bufio"
	"net"

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

	conn   net.Conn
	outbuf chan string
	closed chan struct{}
}

func NewClient() *RedigoClient {
	c := &RedigoClient{
		outbuf: make(chan string, 10),
		closed: make(chan struct{}),
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

func (r *RedigoClient) Init() {
	r.selectDB(0)

	go r.readNextCommand()
	go r.sendReplyToClient()
}

func (r *RedigoClient) selectDB(id int) bool {
	if id < 0 || id > len(r.server.dbs) {
		return false
	} else {
		r.db = r.server.dbs[id]
		return true
	}
}

func (r *RedigoClient) sendReplyToClient() {
	for !r.IsClosed() {
		select {
		case x := <-r.outbuf:
			if _, err := r.conn.Write([]byte(x)); err != nil {
				r.server.RedigoLog(REDIS_VERBOSE, "Error writing to client: %s", err)
				r.Close()
			}
		default:
			if r.Flags&REDIS_CLOSE_AFTER_REPLY > 0 {
				r.Close()
			}
		}

	}
}

func (r *RedigoClient) readNextCommand() {
	/* Return if clients are paused. */

	/* Immediately abort if the client is in the middle of something. */

	/* REDIS_CLOSE_AFTER_REPLY closes the connection once the reply is
	 * written to the client. Make sure to not let the reply grow after
	 * this flag has been set (i.e. don't process more commands). */

	var line string
	scanner := bufio.NewScanner(r.conn)
	for scanner.Scan() {
		line = scanner.Text()
		if line == "" {
			continue
		}

		if line[0] != '*' {
			if arg, err := r.ReadInlineCommand(line, r); err != nil {
				r.AddReplyError(err.Error())
				r.setProtocolError()
			} else {
				r.server.nextToProc <- arg
			}
		} else {
			if arg, err := r.ReadMultiBulkCommand(scanner, r); err != nil {
				r.AddReplyError(err.Error())
				r.setProtocolError()
			} else {
				r.server.nextToProc <- arg
			}
		}
	}
	// if scanner.Err() != nil {
	// 	r.server.RedigoLog(REDIS_VERBOSE, "Error reading from client: %s", scanner.Err())
	// }
	r.Close()
}

func (r *RedigoClient) setProtocolError() {
	r.Flags |= REDIS_CLOSE_AFTER_REPLY
}

func (r *RedigoClient) Close() {
	if !r.IsClosed() {
		r.server.RedigoLog(REDIS_DEBUG, "Closing connection on: %s", r.conn.RemoteAddr())
		r.conn.Close()
		close(r.closed)
		r.server.delClient <- r
	}
}

func (r *RedigoClient) IsClosed() bool {
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
