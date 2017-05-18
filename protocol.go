package redigo

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"strconv"
	"unicode"
)

const (
	REDIS_INLINE_MAXSIZE = 1024 * 60
)

type RESPWriter struct {
	Writer
}

func (r *RESPWriter) AddReply(x []byte) {
	r.Write(x)
}

func (r *RESPWriter) AddReplyInt64(x int64) {
	if x == 0 {
		r.AddReply(CZero)
	} else if x == 1 {
		r.AddReply(COne)
	} else {
		r.AddReply([]byte(fmt.Sprintf(":%d\r\n", x)))
	}
}

func (r *RESPWriter) AddReplyFloat64(x float64) {
	if math.IsInf(x, 0) {
		if x > 0 {
			r.AddReplyBulk([]byte("inf"))
		} else {
			r.AddReplyBulk([]byte("-inf"))
		}
	} else {
		s := fmt.Sprintf("%.17g", x)
		r.AddReply([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)))
	}
}

func (r *RESPWriter) AddReplyMultiBulkLen(x int) {
	r.AddReply([]byte(fmt.Sprintf("*%d\r\n", x)))
}

func (r *RESPWriter) AddReplyBulk(x []byte) {
	r.AddReply([]byte(fmt.Sprintf("$%d\r\n", len(x))))
	r.AddReply(x)
	r.AddReply(CRLF)
}

func (r *RESPWriter) AddReplyError(msg string) {
	r.AddReply([]byte("-ERR "))
	r.AddReply([]byte(msg))
	r.AddReply(CRLF)
}

func (r *RESPWriter) AddReplyStatus(msg string) {
	r.AddReply([]byte("+"))
	r.AddReply([]byte(msg))
	r.AddReply(CRLF)
}

func NewRESPWriter(wr Writer) *RESPWriter {
	return &RESPWriter{wr}
}

type RESPReader struct {
	//argv [][]byte
}

func splitInlineArgs(line []byte) ([][]byte, bool) {
	length := len(line)

	var argv [][]byte
	var inq, insq bool
	for i := 0; i < length; i++ {
		// Skip space
		for unicode.IsSpace(rune(line[i])) {
			i++
		}

		var token []byte
		for ; i < length; i++ {
			if inq {
				if line[i] == '\\' && i+1 < length {
					var c byte
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
					if i+1 < length && !unicode.IsSpace(rune(line[i+1])) {
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
					if i+1 < length && !unicode.IsSpace(rune(line[i+1])) {
						return nil, false
					}
					insq = false
					break
				} else {
					token = append(token, line[i])
				}
			} else {
				var c byte = line[i]
				if c == '"' {
					inq = true
				} else if c == '\'' {
					insq = true
				} else if unicode.IsSpace(rune(c)) {
					break
				} else {
					token = append(token, c)
				}
			}
		}
		argv = append(argv, token)
	}

	// Unterminated quotes
	if inq || insq {
		return nil, false
	}

	return argv, true
}

func (r *RESPReader) ReadInlineCommand(line []byte) (arg CommandArg, err error) {
	if len(line) > REDIS_INLINE_MAXSIZE {
		err = errors.New("Protocol error: too big inline request")
		return
	}

	// Split the input buffer
	if argv, ok := splitInlineArgs(line); !ok {
		err = errors.New("Protocol error: unbalanced quotes in request")
	} else {
		arg.Argc = len(argv)
		arg.Argv = argv
	}
	return
}

func (r *RESPReader) ReadMultiBulkCommand(scanner *bufio.Scanner) (arg CommandArg, err error) {
	var line []byte
	var argv [][]byte

	// Read multi builk length
	line = scanner.Bytes()
	if len(line) > REDIS_INLINE_MAXSIZE {
		err = errors.New("Protocol error: too big mbulk count string")
		return
	}

	// Find out the multi bulk length.
	var mbulklen int
	if x, e := strconv.Atoi(string(line[1:])); e != nil || x > 1024*1024 {
		err = errors.New("Protocol error: invalid multibulk length")
		return
	} else {
		mbulklen = x
	}
	argv = make([][]byte, mbulklen)

	var i int
	for i = 0; i < mbulklen && scanner.Scan(); i++ {
		line = scanner.Bytes()
		if len(line) > REDIS_INLINE_MAXSIZE {
			err = errors.New("Protocol error: too big bulk count string")
			return
		}

		if line[0] != '$' {
			err = errors.New(fmt.Sprintf("Protocol error: expected '$', got '%c'", line[0]))
			return
		}

		if bulklen, e := strconv.Atoi(string(line[1:])); e != nil || bulklen > 512*1024*1024 {
			err = errors.New("Protocol error: invalid bulk length")
			return
		} else {
			if !scanner.Scan() {
				err = errors.New("Protocol error: no bulk data")
				return
			}
			line = scanner.Bytes()
			if len(line) != bulklen {
				err = errors.New("Protocol error: bulk length doesn't match data length")
				return
			}
			argv[i] = line
		}
	}
	if i != mbulklen {
		err = errors.New("Protocol error: multibulk length doesn't match")
		return
	}

	arg.Argc = mbulklen
	arg.Argv = argv
	return
}
