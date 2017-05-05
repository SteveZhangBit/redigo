package redigo

import (
	"bufio"
	"fmt"
	"math"
	"net"
	"strconv"
	"unicode"

	"github.com/SteveZhangBit/redigo/shared"
)

const (
	REDIS_INLINE_MAXSIZE = 1024 * 60
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

type CommandArg struct {
	*RedigoClient

	Argv []string
	Argc int
}

type RedigoClient struct {
	DB     *RedigoDB
	Server *RedigoServer
	Conn   net.Conn
	Flags  int
	outBuf chan string
	closed chan struct{}
}

func NewClient() *RedigoClient {
	return &RedigoClient{
		outBuf: make(chan string, 10),
		closed: make(chan struct{}),
	}
}

func (r *RedigoClient) Init() {
	r.SelectDB(0)

	go r.readNextCommand()
	go r.sendReplyToClient()
}

func (r *RedigoClient) Close() {
	if !r.IsClosed() {
		r.Server.RedigoLog(REDIS_DEBUG, "Closing connection on: %s", r.Conn.RemoteAddr())
		r.Conn.Close()
		close(r.closed)
		r.Server.delClient <- r
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

func (r *RedigoClient) SelectDB(id int) bool {
	if id < 0 || id > len(r.Server.DBs) {
		return false
	} else {
		r.DB = r.Server.DBs[id]
		return true
	}
}

func (r *RedigoClient) sendReplyToClient() {
	for !r.IsClosed() {
		select {
		case x := <-r.outBuf:
			if _, err := r.Conn.Write([]byte(x)); err != nil {
				r.Server.RedigoLog(REDIS_VERBOSE, "Error writing to client: %s", err)
				r.Close()
			}
		default:
			if r.Flags&REDIS_CLOSE_AFTER_REPLY > 0 {
				r.Close()
			}
		}

	}
}

func (r *RedigoClient) AddReply(x string) {
	if r.Flags&REDIS_CLOSE_AFTER_REPLY > 0 {
		return
	}
	r.outBuf <- x
}

func (r *RedigoClient) AddReplyInt64(x int64) {
	if x == 0 {
		r.AddReply(shared.CZero)
	} else if x == 1 {
		r.AddReply(shared.COne)
	} else {
		r.AddReply(fmt.Sprintf(":%d\r\n", x))
	}
}

func (r *RedigoClient) AddReplyFloat64(x float64) {
	if math.IsInf(x, 0) {
		if x > 0 {
			r.AddReplyBulk("inf")
		} else {
			r.AddReplyBulk("-inf")
		}
	} else {
		s := fmt.Sprintf("%.17g", x)
		r.AddReply(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
	}
}

func (r *RedigoClient) AddReplyMultiBulkLen(x int) {
	r.AddReply(fmt.Sprintf("*%d\r\n", x))
}

func (r *RedigoClient) AddReplyBulk(x string) {
	r.AddReply(fmt.Sprintf("$%d\r\n", len(x)))
	r.AddReply(x)
	r.AddReply(shared.CRLF)
}

func (r *RedigoClient) AddReplyError(msg string) {
	r.AddReply("-ERR ")
	r.AddReply(msg)
	r.AddReply(shared.CRLF)
}

func (r *RedigoClient) AddReplyStatus(msg string) {
	r.AddReply("+")
	r.AddReply(msg)
	r.AddReply(shared.CRLF)
}

func (r *RedigoClient) readNextCommand() {
	/* Return if clients are paused. */

	/* Immediately abort if the client is in the middle of something. */

	/* REDIS_CLOSE_AFTER_REPLY closes the connection once the reply is
	 * written to the client. Make sure to not let the reply grow after
	 * this flag has been set (i.e. don't process more commands). */

	var line string
	scanner := bufio.NewScanner(r.Conn)
	for scanner.Scan() {
		line = scanner.Text()
		if line == "" {
			continue
		}

		if line[0] != '*' {
			if arg, ok := r.readInlineCommand(line); ok {
				r.Server.nextToProc <- arg
			}
		} else {
			if arg, ok := r.readMultiBulkCommand(scanner); ok {
				r.Server.nextToProc <- arg
			}
		}
	}
	// if scanner.Err() != nil {
	// 	r.Server.RedigoLog(REDIS_VERBOSE, "Error reading from client: %s", scanner.Err())
	// }
	r.Close()
}

func splitInlineArgs(line []rune) ([]string, bool) {
	length := len(line)

	var argv []string
	var inq, insq bool
	for i := 0; i < length; i++ {
		// Skip space
		for unicode.IsSpace(line[i]) {
			i++
		}

		var token []rune
		for ; i < length; i++ {
			if inq {
				if line[i] == '\\' && i+1 < length {
					var c rune
					i++
					switch line[i] {
					case 'n':
						c = '\n'
					case 'r':
						c = '\r'
					case 't':
						c = '\t'
					case 'b':
						c = '\b'
					case 'a':
						c = '\a'
					default:
						c = line[i]
					}
					token = append(token, c)
				} else if line[i] == '"' {
					if i+1 < length && !unicode.IsSpace(line[i+1]) {
						return nil, false
					}
					inq = false
					break
				} else {
					token = append(token, line[i])
				}
			} else if insq {
				if line[i] == '\\' && i+1 < length && line[i+1] == '\'' {
					i++
					token = append(token, '\'')
				} else if line[i] == '\'' {
					if i+1 < length && !unicode.IsSpace(line[i+1]) {
						return nil, false
					}
					insq = false
					break
				} else {
					token = append(token, line[i])
				}
			} else {
				var c rune = line[i]
				if c == '"' {
					inq = true
				} else if c == '\'' {
					insq = true
				} else if unicode.IsSpace(c) {
					break
				} else {
					token = append(token, c)
				}
			}
		}
		argv = append(argv, string(token))
	}

	// Unterminated quotes
	if inq || insq {
		return nil, false
	}

	return argv, true
}

func (r *RedigoClient) setProtocolError() {
	r.Flags |= REDIS_CLOSE_AFTER_REPLY
}

func (r *RedigoClient) readInlineCommand(line string) (arg CommandArg, success bool) {
	if len(line) > REDIS_INLINE_MAXSIZE {
		r.AddReplyError("Protocol error: too big inline request")
		r.setProtocolError()
		return
	}

	// Split the input buffer
	if argv, ok := splitInlineArgs([]rune(line)); !ok {
		r.AddReplyError("Protocol error: unbalanced quotes in request")
		r.setProtocolError()
		return
	} else {
		arg.Argc = len(argv)
		arg.Argv = argv
		arg.RedigoClient = r
		success = true
		return
	}
}

func (r *RedigoClient) readMultiBulkCommand(scanner *bufio.Scanner) (arg CommandArg, success bool) {
	var line string

	// Read multi builk length
	line = scanner.Text()
	if len(line) > REDIS_INLINE_MAXSIZE {
		r.AddReplyError("Protocol error: too big mbulk count string")
		r.setProtocolError()
		return
	}

	// Find out the multi bulk length.
	var mbulklen int
	if x, err := strconv.ParseInt(line[1:], 10, 64); err != nil || x > 1024*1024 {
		r.AddReplyError("Protocol error: invalid multibulk length")
		r.setProtocolError()
		return
	} else {
		mbulklen = int(x)
	}

	var argv []string
	for ; mbulklen > 0 && scanner.Scan(); mbulklen-- {
		line = scanner.Text()
		if len(line) > REDIS_INLINE_MAXSIZE {
			r.AddReplyError("Protocol error: too big bulk count string")
			r.setProtocolError()
			return
		}

		if line[0] != '$' {
			r.AddReplyError(fmt.Sprintf("Protocol error: expected '$', got '%c'", line[0]))
			r.setProtocolError()
			return
		}

		if bulklen, err := strconv.ParseInt(line[1:], 10, 64); err != nil || bulklen > 512*1024*1024 {
			r.AddReplyError("Protocol error: invalid bulk length")
			r.setProtocolError()
			return
		} else {
			if !scanner.Scan() {
				return
			}
			line = scanner.Text()
			if len(line) != int(bulklen) {
				return
			}
			argv = append(argv, line)
		}
	}
	if mbulklen == 0 {
		arg.Argc = len(argv)
		arg.Argv = argv
		arg.RedigoClient = r
		success = true
		return
	}
	return
}

func (r *RedigoClient) LookupKeyReadOrReply(key string, reply string) interface{} {
	x := r.DB.LookupKeyRead(key)
	if x == nil {
		r.AddReply(reply)
	}
	return x
}

func (r *RedigoClient) LookupKeyWriteOrReply(key string, reply string) interface{} {
	x := r.DB.LookupKeyWrite(key)
	if x == nil {
		r.AddReply(reply)
	}
	return x
}

func CLIENTCommand(c CommandArg) {

}
