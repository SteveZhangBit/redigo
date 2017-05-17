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
	Text  string
	Write func(x string)
}

func (r *RESPWriter) AddReply(x string) {
	r.Write(x)
}

func (r *RESPWriter) AddReplyInt64(x int64) {
	if x == 0 {
		r.AddReply(CZero)
	} else if x == 1 {
		r.AddReply(COne)
	} else {
		r.AddReply(fmt.Sprintf(":%d\r\n", x))
	}
}

func (r *RESPWriter) AddReplyFloat64(x float64) {
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

func (r *RESPWriter) AddReplyMultiBulkLen(x int) {
	r.AddReply(fmt.Sprintf("*%d\r\n", x))
}

func (r *RESPWriter) AddReplyBulk(x string) {
	r.AddReply(fmt.Sprintf("$%d\r\n%s\r\n", len(x), x))
}

func (r *RESPWriter) AddReplyError(msg string) {
	r.AddReply(fmt.Sprintf("-ERR %s\r\n", msg))
}

func (r *RESPWriter) AddReplyStatus(msg string) {
	r.AddReply(fmt.Sprintf("+%s\r\n", msg))
}

func NewRESPWriter() *RESPWriter {
	wr := &RESPWriter{}
	wr.Write = func(x string) {
		wr.Text += x
	}
	return wr
}

func NewRESPWriterFunc(f func(x string)) *RESPWriter {
	return &RESPWriter{Write: f}
}

type RESPReader struct{}

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

func (r *RESPReader) ReadInlineCommand(line string) (arg CommandArg, err error) {
	if len(line) > REDIS_INLINE_MAXSIZE {
		err = errors.New("Protocol error: too big inline request")
		return
	}

	// Split the input buffer
	if argv, ok := splitInlineArgs([]rune(line)); !ok {
		err = errors.New("Protocol error: unbalanced quotes in request")
	} else {
		arg.Argc = len(argv)
		arg.Argv = argv
	}
	return
}

func (r *RESPReader) ReadMultiBulkCommand(scanner *bufio.Scanner) (arg CommandArg, err error) {
	var line string

	// Read multi builk length
	line = scanner.Text()
	if len(line) > REDIS_INLINE_MAXSIZE {
		err = errors.New("Protocol error: too big mbulk count string")
		return
	}

	// Find out the multi bulk length.
	var mbulklen int
	if x, e := strconv.ParseInt(line[1:], 10, 64); e != nil || x > 1024*1024 {
		err = errors.New("Protocol error: invalid multibulk length")
		return
	} else {
		mbulklen = int(x)
	}

	var argv []string
	for ; mbulklen > 0 && scanner.Scan(); mbulklen-- {
		line = scanner.Text()
		if len(line) > REDIS_INLINE_MAXSIZE {
			err = errors.New("Protocol error: too big bulk count string")
			return
		}

		if line[0] != '$' {
			err = errors.New(fmt.Sprintf("Protocol error: expected '$', got '%c'", line[0]))
			return
		}

		if bulklen, e := strconv.ParseInt(line[1:], 10, 64); e != nil || bulklen > 512*1024*1024 {
			err = errors.New("Protocol error: invalid bulk length")
			return
		} else {
			if !scanner.Scan() {
				err = errors.New("Protocol error: no bulk data")
				return
			}
			line = scanner.Text()
			if len(line) != int(bulklen) {
				err = errors.New("Protocol error: bulk length doesn't match data length")
				return
			}
			argv = append(argv, line)
		}
	}
	if mbulklen != 0 {
		err = errors.New("Protocol error: multibulk length doesn't match")
		return
	}

	arg.Argc = len(argv)
	arg.Argv = argv
	return
}
