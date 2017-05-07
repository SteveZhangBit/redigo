package command

import (
	"fmt"
	"math"
	"testing"

	"github.com/SteveZhangBit/redigo"
)

func TestSET(t *testing.T) {
	fake := NewFakeClient()

	SETCommand(NewCommand(fake, "set", "foo", "bar"))
	if fake.server.Dirty != 1 {
		t.Error("set: add dirty")
	}
	if fake.CompareText(redigo.OK, t) {
		t.Error("set: reply ok")
	}

	SETCommand(NewCommand(fake, "set", "foo", "barr", "nx"))
	if fake.CompareText(redigo.NullBulk, t) {
		t.Error("set: when nx is set and foo exists")
	}

	SETCommand(NewCommand(fake, "set", "bar", "foo", "nx"))
	if fake.CompareText(redigo.OK, t) {
		t.Error("set: when nx is set and bar not exists")
	}

	SETNXCommand(NewCommand(fake, "setnx", "bar", "foo"))
	if fake.CompareText(redigo.CZero, t) {
		t.Error("setnx: when bar exists")
	}
}

func TestGET(t *testing.T) {
	fake := NewFakeClient()
	cmd := NewCommand(fake, "get", "foo")

	GETCommand(cmd)
	if fake.CompareText(redigo.NullBulk, t) {
		t.Error("get: not exists")
	}

	fake.db.Dict["foo"] = 1
	GETCommand(cmd)
	if fake.CompareText(redigo.WrongTypeErr, t) {
		t.Error("get: wrong type")
	}

	SETCommand(NewCommand(fake, "set", "foo", "bar"))
	fake.ReplyText = ""
	GETCommand(cmd)
	if fake.CompareText("$3\r\nbar\r\n", t) {
		t.Error("get:")
	}
}

func TestINCRBYFLOAT(t *testing.T) {
	fake := NewFakeClient()

	INCRBYFLOATCommand(NewCommand(fake, "incrbyfloat", "foo", "0.5"))
	if fake.server.Dirty != 1 {
		t.Error("incrbyfloat: add dirty")
	}
	f := fmt.Sprintf("%.17f", 0.5)
	if fake.CompareText(fmt.Sprintf("$%d\r\n%s\r\n", len(f), f), t) {
		t.Error("incrbyfloat: when foo not exists")
	}

	INCRBYFLOATCommand(NewCommand(fake, "incrbyfloat", "foo", "0.9"))
	f = fmt.Sprintf("%.17f", 0.5+0.9)
	fbulk := fmt.Sprintf("$%d\r\n%s\r\n", len(f), f)
	if fake.CompareText(fbulk, t) {
		t.Error("incrbyfloat: when foo exists")
	}
	GETCommand(NewCommand(fake, "get", "foo"))
	if fake.CompareText(fbulk, t) {
		t.Error("incrbyfloat: when set new value")
	}

	INCRBYFLOATCommand(NewCommand(fake, "incrbyfloat", "foo", fmt.Sprintf("%f", math.Inf(0))))
	if fake.CompareText("-ERR increment would produce NaN or Infinity", t) {
		t.Error("incrbyfloat: when increment produce Nan or Inf")
	}

	SETCommand(NewCommand(fake, "set", "foo", "bar"))
	fake.ReplyText = ""
	INCRBYFLOATCommand(NewCommand(fake, "incrbyfloat", "foo", "0.5"))
	if fake.CompareText("-ERR value is not a valid float", t) {
		t.Error("incrbyfloat: when bar is not a float")
	}

	SETCommand(NewCommand(fake, "set", "foo", "0.5"))
	fake.ReplyText = ""
	INCRBYFLOATCommand(NewCommand(fake, "incrbyfloat", "foo", "bar"))
	if fake.CompareText("-ERR value is not a valid float", t) {
		t.Error("incrbyfloat: when c.Argv[2] is not a float")
	}
}

func TestINCR(t *testing.T) {
	fake := NewFakeClient()

	INCRCommand(NewCommand(fake, "incr", "foo"))
	if fake.CompareText(":1\r\n", t) {
		t.Error("incr: when foo not exists")
	}

	DECRCommand(NewCommand(fake, "decr", "foo"))
	if fake.CompareText(":0\r\n", t) {
		t.Error("decr: when foo exists")
	}

	INCRBYCommand(NewCommand(fake, "incrby", "foo", "3"))
	if fake.CompareText(":3\r\n", t) {
		t.Error("incrby: fail to add 3 on foo")
	}

	INCRBYCommand(NewCommand(fake, "incrby", "foo", "bar"))
	if fake.CompareText("-ERR value is not an integer or out of range", t) {
		t.Error("incrby: when c.Argv[2] is not an integer")
	}

	INCRBYCommand(NewCommand(fake, "incrby", "foo", fmt.Sprintf("%d", math.MaxInt64)))
	if fake.CompareText("-ERR increment or decrement would overflow", t) {
		t.Error("incrby: when overflow")
	}

	SETCommand(NewCommand(fake, "set", "foo", "bar"))
	fake.ReplyText = ""
	INCRCommand(NewCommand(fake, "incr", "foo"))
	if fake.CompareText("-ERR value is not an integer or out of range", t) {
		t.Error("incr: when foo is not an integer")
	}
}

func TestAPPEND(t *testing.T) {
	fake := NewFakeClient()

	APPENDCommand(NewCommand(fake, "append", "foo", "hello"))
	if fake.CompareText("5", t) {
		t.Error("append: when foo not exists")
	}
	GETCommand(NewCommand(fake, "get", "foo"))
	if fake.CompareText("$5\r\nhello\r\n", t) {
		t.Error("append: fail to set foo")
	}

	APPENDCommand(NewCommand(fake, "append", "foo", " world"))
	if fake.CompareText("11", t) {
		t.Error("append: when foo exists")
	}
	GETCommand(NewCommand(fake, "get", "foo"))
	if fake.CompareText("$11\r\nhello world\r\n", t) {
		t.Error("append: fail to update foo")
	}
}

func TestSTRLEN(t *testing.T) {
	fake := NewFakeClient()

	STRLENCommand(NewCommand(fake, "strlen", "foo"))
	if fake.CompareText(redigo.CZero, t) {
		t.Error("strlen: when foo not exists")
	}

	SETCommand(NewCommand(fake, "set", "foo", "bar"))
	fake.ReplyText = ""
	STRLENCommand(NewCommand(fake, "strlen", "foo"))
	if fake.CompareText("3", t) {
		t.Error("strlen: when foo exists")
	}
}
