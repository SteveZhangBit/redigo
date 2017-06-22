package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"unicode"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/util"
)

const (
	REDIS_INLINE_MAXSIZE = 1024 * 60
)

const (
	REDIS_SHARED_INTEGERS    = 10000
	REDIS_SHARED_BULKHDR_LEN = 32
)

var (
	CRLF           = []byte("\r\n")
	OK             = []byte("+OK\r\n")
	CZero          = []byte(":0\r\n")
	COne           = []byte(":1\r\n")
	CNegOne        = []byte(":-1\r\n")
	NullBulk       = []byte("$-1\r\n")
	NullMultiBulk  = []byte("*-1\r\n")
	EmptyMultiBulk = []byte("*0\r\n")
	Pong           = []byte("+PONG\r\n")
	WrongTypeErr   = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	SyntaxErr      = []byte("-ERR syntax error\r\n")
	NoKeyErr       = []byte("-ERR no such key\r\n")
	OutOfRangeErr  = []byte("-ERR index out of range\r\n")
	SameObjectErr  = []byte("-ERR source and destination objects are the same\r\n")
)

var (
	SharedIntegers [][]byte
	Sharedmbulkhdr [][]byte
	Sharedbulkhdr  [][]byte
)

func init() {
	SharedIntegers = make([][]byte, REDIS_SHARED_INTEGERS)
	for i := 0; i < REDIS_SHARED_INTEGERS; i++ {
		SharedIntegers[i] = []byte(strconv.Itoa(i))
	}

	Sharedmbulkhdr = make([][]byte, REDIS_SHARED_BULKHDR_LEN)
	Sharedbulkhdr = make([][]byte, REDIS_SHARED_BULKHDR_LEN)
	for i := 0; i < REDIS_SHARED_BULKHDR_LEN; i++ {
		Sharedmbulkhdr[i] = []byte(fmt.Sprintf("*%d\r\n", i))
		Sharedbulkhdr[i] = []byte(fmt.Sprintf("$%d\r\n", i))
	}
}

type RESPWriter struct {
	*bufio.Writer
	err error
}

func NewRESPWriter(w io.Writer) *RESPWriter {
	return &RESPWriter{Writer: bufio.NewWriter(w)}
}

func (r *RESPWriter) AddReply(x []byte) {
	if r.err == nil {
		_, r.err = r.Write(x)
	}
}

func (r *RESPWriter) AddReplyByte(x byte) {
	if r.err == nil {
		r.err = r.WriteByte(x)
	}
}

func (r *RESPWriter) AddReplyString(x string) {
	if r.err == nil {
		_, r.err = r.WriteString(x)
	}
}

func (r *RESPWriter) Flush() error {
	if r.err == nil {
		r.err = r.Writer.Flush()
	}
	return r.err
}

func (r *RESPWriter) AddReplyInt64(x int64) {
	if x == 0 {
		r.AddReply(CZero)
	} else if x == 1 {
		r.AddReply(COne)
	} else if x < REDIS_SHARED_INTEGERS {
		r.AddReplyByte(':')
		r.AddReply(SharedIntegers[x])
		r.AddReply(CRLF)
	} else {
		r.AddReplyByte(':')
		r.AddReplyString(strconv.FormatInt(x, 10))
		r.AddReply(CRLF)
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
		r.AddReplyBulk([]byte(strconv.FormatFloat(x, 'g', 17, 64)))
	}
}

func (r *RESPWriter) AddReplyMultiBulkLen(x int) {
	if x < REDIS_SHARED_BULKHDR_LEN {
		r.AddReply(Sharedmbulkhdr[x])
	} else {
		r.AddReplyByte('*')
		r.AddReplyString(strconv.Itoa(x))
		r.AddReply(CRLF)
	}
}

func (r *RESPWriter) AddReplyBulk(x []byte) {
	if len(x) < REDIS_SHARED_BULKHDR_LEN {
		r.AddReply(Sharedbulkhdr[len(x)])
	} else {
		r.AddReplyByte('$')
		r.AddReplyString(strconv.Itoa(len(x)))
		r.AddReply(CRLF)
	}
	r.AddReply(x)
	r.AddReply(CRLF)
}

func (r *RESPWriter) AddReplyError(msg string) {
	r.AddReplyString("-ERR ")
	r.AddReplyString(msg)
	r.AddReply(CRLF)
}

func (r *RESPWriter) AddReplyStatus(msg string) {
	r.AddReplyByte('+')
	r.AddReplyString(msg)
	r.AddReply(CRLF)
}

type RESPReader struct {
	argv    [][]byte
	scanner *bufio.Scanner
}

func NewRESPReader(r io.Reader) *RESPReader {
	return &RESPReader{argv: make([][]byte, 4), scanner: bufio.NewScanner(r)}
}

func (r *RESPReader) Read() (arg *redigo.CommandArg, err error) {
	if !r.scanner.Scan() {
		err = io.EOF
		return
	}

	for {
		line := r.scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		if line[0] != '*' {
			arg, err = r.ReadInlineCommand(line)
		} else {
			arg, err = r.ReadMultiBulkCommand()
		}
		return
	}
}

func (r *RESPReader) splitInlineArgs(line []byte) ([][]byte, bool) {
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

func (r *RESPReader) ReadInlineCommand(line []byte) (arg *redigo.CommandArg, err error) {
	if len(line) > REDIS_INLINE_MAXSIZE {
		err = errors.New("Protocol error: too big inline request")
		return
	}

	// Split the input buffer
	if argv, ok := r.splitInlineArgs(line); !ok {
		err = errors.New("Protocol error: unbalanced quotes in request")
	} else {
		arg = &redigo.CommandArg{Argc: len(argv), Argv: argv}
	}
	return
}

func (r *RESPReader) ReadMultiBulkCommand() (arg *redigo.CommandArg, err error) {
	var line []byte

	// Read multi builk length
	line = r.scanner.Bytes()
	if len(line) > REDIS_INLINE_MAXSIZE {
		err = errors.New("Protocol error: too big mbulk count string")
		return
	}

	// Find out the multi bulk length.
	var mbulklen int64
	if x, ok := util.ParseInt(line[1:], 10, 64); !ok || x > 1024*1024 {
		err = errors.New("Protocol error: invalid multibulk length")
		return
	} else {
		mbulklen = x
	}

	var i int64

	argv_len := int64(len(r.argv))
	for i = 0; i < mbulklen && r.scanner.Scan(); i++ {
		line = r.scanner.Bytes()
		if len(line) > REDIS_INLINE_MAXSIZE {
			err = errors.New("Protocol error: too big bulk count string")
			return
		}

		if line[0] != '$' {
			err = errors.New(fmt.Sprintf("Protocol error: expected '$', got '%c'", line[0]))
			return
		}

		if bulklen, ok := util.ParseInt(line[1:], 10, 64); !ok || bulklen > 512*1024*1024 {
			err = errors.New("Protocol error: invalid bulk length")
			return
		} else {
			if !r.scanner.Scan() {
				err = errors.New("Protocol error: no bulk data")
				return
			}
			line = r.scanner.Bytes()
			if int64(len(line)) != bulklen {
				err = errors.New("Protocol error: bulk length doesn't match data length")
				return
			}
			if i < argv_len {
				r.argv[i] = line
			} else {
				r.argv = append(r.argv, line)
			}
		}
	}
	if i != mbulklen {
		err = errors.New("Protocol error: multibulk length doesn't match")
		return
	}

	arg = &redigo.CommandArg{Argc: int(mbulklen), Argv: r.argv}
	return
}
