package command

import (
	"testing"
	"time"

	"github.com/SteveZhangBit/redigo"
)

type TestClient struct {
	*redigo.RESPWriter
	*redigo.RESPReader

	*TestPubSub

	db     *TestDB
	server *TestServer
}

func (c *TestClient) CompareText(t *testing.T, x string) bool {
	ok := c.Text != x
	if ok {
		t.Logf("need %q, get %q", x, c.Text)
	}
	c.Text = ""
	return ok
}

func (c *TestClient) CompareInt64(t *testing.T, x int64) bool {
	writer := redigo.NewRESPWriter()
	writer.AddReplyInt64(x)
	return c.CompareText(t, writer.Text)
}

func (c *TestClient) CompareFloat64(t *testing.T, x float64) bool {
	writer := redigo.NewRESPWriter()
	writer.AddReplyFloat64(x)
	return c.CompareText(t, writer.Text)
}

func (c *TestClient) CompareErr(t *testing.T, msg string) bool {
	writer := redigo.NewRESPWriter()
	writer.AddReplyError(msg)
	return c.CompareText(t, writer.Text)
}

func (c *TestClient) CompareBulk(t *testing.T, x string) bool {
	writer := redigo.NewRESPWriter()
	writer.AddReplyBulk(x)
	return c.CompareText(t, writer.Text)
}

func (c *TestClient) CompareMultiBulk(t *testing.T, xs ...string) bool {
	writer := redigo.NewRESPWriter()
	writer.AddReplyMultiBulkLen(len(xs))
	for _, x := range xs {
		writer.AddReplyBulk(x)
	}
	return c.CompareText(t, writer.Text)
}

func (c *TestClient) DB() redigo.DB {
	return c.db
}

func (c *TestClient) Server() redigo.Server {
	return c.server
}

func (c *TestClient) SelectDB(id int) bool {
	return true
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

func (c *TestClient) BlockForKeys(keys []string, timeout time.Duration) {

}

type TestServer struct {
	Closed bool
	Dirty  int
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

func (d *TestDB) GetDict() map[string]interface{} {
	return d.Dict
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

func (d *TestDB) ExpireIfNeed(key string) bool {
	return false
}

func (d *TestDB) GetExpire(key string) time.Duration {
	return time.Duration(-1)
}

func (d *TestDB) SetExpire(key string, t time.Duration) {

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
	return &TestClient{RESPWriter: redigo.NewRESPWriter(), server: &TestServer{}, db: &TestDB{Dict: make(map[string]interface{})}}
}

func TestPING(t *testing.T) {
	fake := NewFakeClient()

	PINGCommand(NewCommand(fake, "ping", "hello", "world"))
	if fake.CompareErr(t, "wrong number of arguments for 'ping' command") {
		t.Error("ping: c.Argc > 2")
	}

	PINGCommand(NewCommand(fake, "ping", "hello"))
	if fake.CompareBulk(t, "hello") {
		t.Error("ping: c.Argc == 2")
	}

	PINGCommand(NewCommand(fake, "ping"))
	if fake.CompareText(t, redigo.Pong) {
		t.Error("ping: failed")
	}
}

func TestSHUTDOWN(t *testing.T) {
	fake := NewFakeClient()

	SHUTDOWNCommand(NewCommand(fake, "shutdown", "right", "now"))
	if fake.CompareText(t, redigo.SyntaxErr) {
		t.Error("shutdown: c.Argc > 2")
	}
	SHUTDOWNCommand(NewCommand(fake, "shutdown"))
	if fake.CompareErr(t, "Errors trying to SHUTDOWN. Check logs.") {
		t.Error("shutdown: PrepareForShutdown false")
	}
}
