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

type RedigoClient struct {
	DB     *RedigoDB
	Server *RedigoServer
	Argv   []string
	Argc   int
	Conn   net.Conn
}

func NewClient() *RedigoClient {
	return &RedigoClient{}
}

func (r *RedigoClient) Init() {
	r.SelectDB(0)
	go r.readNextCommand()
}

func (r *RedigoClient) SelectDB(id int) bool {
	if id < 0 || id > len(r.Server.DBs) {
		return false
	} else {
		r.DB = r.Server.DBs[id]
		return true
	}
}

func (r *RedigoClient) AddReply(x string) {
	go func() {
		if _, err := r.Conn.Write([]byte(x)); err != nil {
			r.Server.RedigoLog(REDIS_VERBOSE, "Error writing to client: ", err)
			r.Close()
		}
	}()
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

	scanner := bufio.NewScanner(r.Conn)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		if line[0] != '*' {
			if r.readInlineCommand(line) {
				r.Server.nextToProc <- r
			}
		} else {
			if r.readMultiBulkCommand(scanner) {
				r.Server.nextToProc <- r
			}
		}
	}
	if scanner.Err() != nil {
		r.Server.RedigoLog(REDIS_VERBOSE, "Error reading from client: ", scanner.Err())
	}
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
				if line[i] == '\\' && i+1 < length {
					if line[i+1] == '\'' {
						i++
						token = append(token, '\'')
					}
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

func (r *RedigoClient) readInlineCommand(line string) bool {
	if len(line) > REDIS_INLINE_MAXSIZE {
		r.AddReplyError("Protocol error: too big inline request")
		return false
	}

	// Split the input buffer
	var argv []string
	var ok bool
	if argv, ok = splitInlineArgs([]rune(line)); !ok {
		r.AddReplyError("Protocol error: unbalanced quotes in request")
		return false
	}

	r.Argc = len(argv)
	r.Argv = argv
	return true
}

func (r *RedigoClient) readMultiBulkCommand(scanner *bufio.Scanner) bool {
	var line string
	// Read multi builk length
	line = scanner.Text()
	if len(line) > REDIS_INLINE_MAXSIZE {
		r.AddReplyError("Protocol error: too big mbulk count string")
		return false
	}

	// Find out the multi bulk length.
	var mbulklen int
	if x, err := strconv.ParseInt(line[1:], 10, 64); err != nil || x > 1024*1024 {
		r.AddReplyError("Protocol error: invalid multibulk length")
		return false
	} else {
		mbulklen = int(x)
	}

	var argv []string
	for ; mbulklen > 0 && scanner.Scan(); mbulklen-- {
		line = scanner.Text()
		if len(line) > REDIS_INLINE_MAXSIZE {
			r.AddReplyError("Protocol error: too big bulk count string")
			return false
		}

		if line[0] != '$' {
			r.AddReplyError(fmt.Sprintf("Protocol error: expected '$', got '%c'", line[0]))
			return false
		}

		if bulklen, err := strconv.ParseInt(line[1:], 10, 64); err != nil || bulklen > 512*1024*1024 {
			r.AddReplyError("Protocol error: invalid bulk length")
			return false
		} else {
			if !scanner.Scan() {
				return false
			}
			line = scanner.Text()
			if len(line) != int(bulklen) {
				return false
			}
			argv = append(argv, line)
		}
	}
	if mbulklen == 0 {
		r.Argc = len(argv)
		r.Argv = argv
		return true
	}
	return false
}

func (r *RedigoClient) LookupKeyReadOrReply(key string, reply string) interface{} {
	x := r.DB.LookupKeyRead(key)
	if x != nil {
		r.AddReply(reply)
	}
	return x
}

func (r *RedigoClient) LookupKeyWriteOrReply(key string, reply string) interface{} {
	x := r.DB.LookupKeyWrite(key)
	if x != nil {
		r.AddReply(reply)
	}
	return x
}

func (r *RedigoClient) Close() {
	r.Conn.Close()
	r.Server.delClient <- r
}

func CLIENTCommand(c *RedigoClient) {

}
