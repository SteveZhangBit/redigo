package command

import (
	"bytes"
	"testing"
	"time"

	"github.com/SteveZhangBit/redigo"
)

type TextWriter struct {
	Text []byte
}

func (w *TextWriter) Write(b []byte) {
	w.Text = append(w.Text, b...)
}

func (w *TextWriter) WriteString(x string) {
	w.Write([]byte(x))
}

func (w *TextWriter) Flush() {
	w.Text = nil
}

type TestClient struct {
	*redigo.RESPWriter
	*redigo.RESPReader

	*TestPubSub

	db     *TestDB
	server *TestServer
}

func (c *TestClient) Text() []byte {
	return c.RESPWriter.Writer.(*TextWriter).Text
}

func (c *TestClient) CompareText(t *testing.T, x interface{}) (ok bool) {
	switch s := x.(type) {
	case string:
		ok = string(c.Text()) != s
	case []byte:
		ok = !bytes.Equal(c.Text(), s)
	}

	if ok {
		t.Logf("need %q, get %q", x, string(c.Text()))
	}
	c.Flush()
	return
}

func (c *TestClient) CompareInt64(t *testing.T, x int64) bool {
	writer := redigo.NewRESPWriter(&TextWriter{})
	writer.AddReplyInt64(x)
	return c.CompareText(t, writer.Writer.(*TextWriter).Text)
}

func (c *TestClient) CompareFloat64(t *testing.T, x float64) bool {
	writer := redigo.NewRESPWriter(&TextWriter{})
	writer.AddReplyFloat64(x)
	return c.CompareText(t, writer.Writer.(*TextWriter).Text)
}

func (c *TestClient) CompareErr(t *testing.T, msg string) bool {
	writer := redigo.NewRESPWriter(&TextWriter{})
	writer.AddReplyError(msg)
	return c.CompareText(t, writer.Writer.(*TextWriter).Text)
}

func (c *TestClient) CompareBulk(t *testing.T, x string) bool {
	writer := redigo.NewRESPWriter(&TextWriter{})
	writer.AddReplyBulk([]byte(x))
	return c.CompareText(t, writer.Writer.(*TextWriter).Text)
}

func (c *TestClient) CompareMultiBulk(t *testing.T, xs ...string) bool {
	writer := redigo.NewRESPWriter(&TextWriter{})
	writer.AddReplyMultiBulkLen(len(xs))
	for _, x := range xs {
		writer.AddReplyBulk([]byte(x))
	}
	return c.CompareText(t, writer.Writer.(*TextWriter).Text)
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

func (c *TestClient) LookupKeyReadOrReply(key []byte, reply []byte) interface{} {
	x := c.db.LookupKeyRead(key)
	if x == nil {
		c.AddReply(reply)
	}
	return x
}

func (c *TestClient) LookupKeyWriteOrReply(key []byte, reply []byte) interface{} {
	x := c.db.LookupKeyWrite(key)
	if x == nil {
		c.AddReply(reply)
	}
	return x
}

func (c *TestClient) BlockForKeys(keys [][]byte, timeout time.Duration) {

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

func (d *TestDB) LookupKey(key []byte) interface{} {
	o, _ := d.Dict[string(key)]
	return o
}

func (d *TestDB) LookupKeyRead(key []byte) interface{} {
	return d.LookupKey(key)
}

func (d *TestDB) LookupKeyWrite(key []byte) interface{} {
	return d.LookupKey(key)
}

func (d *TestDB) Add(key []byte, val interface{}) {
	d.Dict[string(key)] = val
}

func (d *TestDB) Update(key []byte, val interface{}) {
	d.Dict[string(key)] = val
}

func (d *TestDB) Delete(key []byte) (ok bool) {
	if _, ok = d.Dict[string(key)]; ok {
		delete(d.Dict, string(key))
	}
	return
}

func (d *TestDB) SetKeyPersist(key []byte, val interface{}) {
	d.Dict[string(key)] = val
}

func (d *TestDB) Exists(key []byte) (ok bool) {
	_, ok = d.Dict[string(key)]
	return
}

func (d *TestDB) RandomKey() (key []byte) {
	for key := range d.Dict {
		return []byte(key)
	}
	return []byte{}
}

func (d *TestDB) SignalModifyKey(key []byte) {

}

func (d *TestDB) ExpireIfNeed(key []byte) bool {
	return false
}

func (d *TestDB) GetExpire(key []byte) time.Duration {
	return time.Duration(-1)
}

func (d *TestDB) SetExpire(key []byte, t time.Duration) {

}

type TestPubSub struct{}

func (p *TestPubSub) NotifyKeyspaceEvent(t int, event string, key []byte, dbid int) {
	if p == nil {
	}
}

func NewCommand(fake redigo.Client, argv ...string) redigo.CommandArg {
	argv_bytes := make([][]byte, len(argv))
	for i, s := range argv {
		argv_bytes[i] = []byte(s)
	}
	return redigo.CommandArg{Argc: len(argv), Argv: argv_bytes, Client: fake}
}

func NewFakeClient() *TestClient {
	return &TestClient{
		RESPWriter: redigo.NewRESPWriter(&TextWriter{}),
		server:     &TestServer{},
		db:         &TestDB{Dict: make(map[string]interface{})},
	}
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
