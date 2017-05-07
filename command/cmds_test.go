package command

import (
	"fmt"
	"testing"

	"github.com/SteveZhangBit/redigo"
)

type TestClient struct {
	*TestPubSub

	db     *TestDB
	server *TestServer

	ReplyText string
}

func (c *TestClient) CompareText(x string, t *testing.T) bool {
	ok := c.ReplyText != x
	if ok {
		t.Logf("need %q, get %q", x, c.ReplyText)
	}
	c.ReplyText = ""
	return ok
}

func (c *TestClient) AddReply(x string) {
	c.ReplyText += x
}

func (c *TestClient) AddReplyInt64(x int64) {
	c.ReplyText += fmt.Sprintf("%d", x)
}

func (c *TestClient) AddReplyFloat64(x float64) {
	c.ReplyText += fmt.Sprintf("%.17f", x)
}

func (c *TestClient) AddReplyMultiBulkLen(x int) {
	c.ReplyText += fmt.Sprintf("*%d\r\n", x)
}

func (c *TestClient) AddReplyBulk(x string) {
	c.ReplyText += fmt.Sprintf("$%d\r\n%s\r\n", len(x), x)
}

func (c *TestClient) AddReplyError(msg string) {
	c.ReplyText += fmt.Sprintf("-ERR %s", msg)
}

func (c *TestClient) AddReplyStatus(msg string) {
	c.ReplyText += fmt.Sprintf("+%s", msg)
}

func (c *TestClient) DB() redigo.DB {
	return c.db
}

func (c *TestClient) Server() redigo.Server {
	return c.server
}

func (c *TestClient) Init() {

}

func (c *TestClient) Close() {

}

func (c *TestClient) IsClosed() bool {
	return false
}

func (c *TestClient) LookupKeyReadOrReply(key string, reply string) interface{} {
	x := c.db.LookupKeyRead(key)
	if x == nil {
		c.AddReply(reply)
	}
	return x
}

func (c *TestClient) LookupKeyWriteOrReply(key string, reply string) interface{} {
	x := c.db.LookupKeyWrite(key)
	if x == nil {
		c.AddReply(reply)
	}
	return x
}

type TestServer struct {
	Closed bool
	Dirty  int
}

func (s *TestServer) Init() {

}

func (s *TestServer) PrepareForShutdown() bool {
	return s.Closed
}

func (s *TestServer) AddDirty(i int) {
	s.Dirty = i
}

type TestDB struct {
	Dict map[string]interface{}
}

func (d *TestDB) GetID() int {
	return 0
}

func (d *TestDB) LookupKey(key string) interface{} {
	o, _ := d.Dict[key]
	return o
}

func (d *TestDB) LookupKeyRead(key string) interface{} {
	return d.LookupKey(key)
}

func (d *TestDB) LookupKeyWrite(key string) interface{} {
	return d.LookupKey(key)
}

func (d *TestDB) Add(key string, val interface{}) {
	d.Dict[key] = val
}

func (d *TestDB) Update(key string, val interface{}) {
	d.Dict[key] = val
}

func (d *TestDB) Delete(key string) (ok bool) {
	if _, ok = d.Dict[key]; ok {
		delete(d.Dict, key)
	}
	return
}

func (d *TestDB) SetKeyPersist(key string, val interface{}) {
	d.Dict[key] = val
}

func (d *TestDB) Exists(key string) (ok bool) {
	_, ok = d.Dict[key]
	return
}

func (d *TestDB) RandomKey() (key string) {
	for key := range d.Dict {
		return key
	}
	return ""
}

func (d *TestDB) SignalModifyKey(key string) {

}

func (d *TestDB) SignalListAsReady(key string) {

}

type TestPubSub struct{}

func (p *TestPubSub) NotifyKeyspaceEvent(t int, event string, key string, dbid int) {
	if p == nil {
	}
}

func NewCommand(fake redigo.Client, argv ...string) redigo.CommandArg {
	return redigo.CommandArg{Argc: len(argv), Argv: argv, Client: fake}
}

func NewFakeClient() *TestClient {
	return &TestClient{server: &TestServer{}, db: &TestDB{Dict: make(map[string]interface{})}}
}

func TestPING(t *testing.T) {
	fake := NewFakeClient()

	PINGCommand(NewCommand(fake, "ping", "hello", "world"))
	if fake.CompareText("-ERR wrong number of arguments for 'ping' command", t) {
		t.Error("ping: c.Argc > 2")
	}

	PINGCommand(NewCommand(fake, "ping", "hello"))
	if fake.CompareText("$5\r\nhello\r\n", t) {
		t.Error("ping: c.Argc == 2")
	}

	PINGCommand(NewCommand(fake, "ping"))
	if fake.CompareText(redigo.Pong, t) {
		t.Error("ping: failed")
	}
}

func TestSHUTDOWN(t *testing.T) {
	fake := NewFakeClient()

	SHUTDOWNCommand(NewCommand(fake, "shutdown", "right", "now"))
	if fake.CompareText(redigo.SyntaxErr, t) {
		t.Error("shutdown: c.Argc > 2")
	}
	SHUTDOWNCommand(NewCommand(fake, "shutdown"))
	if fake.CompareText("-ERR Errors trying to SHUTDOWN. Check logs.", t) {
		t.Error("shutdown: PrepareForShutdown false")
	}
}
