package redigo

import (
	"fmt"

	"github.com/SteveZhangBit/redigo/shared"
)

type RedigoClient struct {
	DB     *RedigoDB
	Server *RedigoServer
	Argv   []string
	Argc   int
}

func (r *RedigoClient) AddReply(x string) {

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

}

func (r *RedigoClient) AddReplyMultiBulkLen(x int) {
	r.AddReply(fmt.Sprintf("*%d", x))
}

func (r *RedigoClient) AddReplyBulk(x string) {
	r.AddReply(fmt.Sprintf("$%d", len(x)))
	r.AddReply(x)
	r.AddReply(shared.CRLF)
}

func (r *RedigoClient) AddReplyError(msg string) {
	r.AddReply("-ERR ")
	r.AddReply(msg)
	r.AddReply(shared.CRLF)
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
