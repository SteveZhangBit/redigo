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
	if fake.CompareText(t, redigo.OK) {
		t.Error("set: reply ok")
	}

	SETCommand(NewCommand(fake, "set", "foo", "barr", "nx"))
	if fake.CompareText(t, redigo.NullBulk) {
		t.Error("set: when nx is set and foo exists")
	}

	SETCommand(NewCommand(fake, "set", "bar", "foo", "nx"))
	if fake.CompareText(t, redigo.OK) {
		t.Error("set: when nx is set and bar not exists")
	}

	SETNXCommand(NewCommand(fake, "setnx", "bar", "foo"))
	if fake.CompareText(t, redigo.CZero) {
		t.Error("setnx: when bar exists")
	}
}

func TestGET(t *testing.T) {
	fake := NewFakeClient()
	cmd := NewCommand(fake, "get", "foo")

	GETCommand(cmd)
	if fake.CompareText(t, redigo.NullBulk) {
		t.Error("get: not exists")
	}

	fake.db.Dict["foo"] = 1
	GETCommand(cmd)
	if fake.CompareText(t, redigo.WrongTypeErr) {
		t.Error("get: wrong type")
	}

	SETCommand(NewCommand(fake, "set", "foo", "bar"))
	fake.Flush()
	GETCommand(cmd)
	if fake.CompareBulk(t, "bar") {
		t.Error("get:")
	}
}

func TestINCRBYFLOAT(t *testing.T) {
	fake := NewFakeClient()

	INCRBYFLOATCommand(NewCommand(fake, "incrbyfloat", "foo", "0.5"))
	if fake.server.Dirty != 1 {
		t.Error("incrbyfloat: add dirty")
	}
	if fake.CompareFloat64(t, 0.5) {
		t.Error("incrbyfloat: when foo not exists")
	}

	INCRBYFLOATCommand(NewCommand(fake, "incrbyfloat", "foo", "0.9"))
	if fake.CompareFloat64(t, 0.5+0.9) {
		t.Error("incrbyfloat: when foo exists")
	}
	GETCommand(NewCommand(fake, "get", "foo"))
	if fake.CompareFloat64(t, 0.5+0.9) {
		t.Error("incrbyfloat: when set new value")
	}

	INCRBYFLOATCommand(NewCommand(fake, "incrbyfloat", "foo", fmt.Sprintf("%f", math.Inf(0))))
	if fake.CompareErr(t, "increment would produce NaN or Infinity") {
		t.Error("incrbyfloat: when increment produce Nan or Inf")
	}

	SETCommand(NewCommand(fake, "set", "foo", "bar"))
	fake.Flush()
	INCRBYFLOATCommand(NewCommand(fake, "incrbyfloat", "foo", "0.5"))
	if fake.CompareErr(t, "value is not a valid float") {
		t.Error("incrbyfloat: when bar is not a float")
	}

	SETCommand(NewCommand(fake, "set", "foo", "0.5"))
	fake.Flush()
	INCRBYFLOATCommand(NewCommand(fake, "incrbyfloat", "foo", "bar"))
	if fake.CompareErr(t, "value is not a valid float") {
		t.Error("incrbyfloat: when c.Argv[2] is not a float")
	}
}

func TestINCR(t *testing.T) {
	fake := NewFakeClient()

	INCRCommand(NewCommand(fake, "incr", "foo"))
	if fake.CompareInt64(t, 1) {
		t.Error("incr: when foo not exists")
	}

	DECRCommand(NewCommand(fake, "decr", "foo"))
	if fake.CompareInt64(t, 0) {
		t.Error("decr: when foo exists")
	}

	INCRBYCommand(NewCommand(fake, "incrby", "foo", "3"))
	if fake.CompareInt64(t, 3) {
		t.Error("incrby: fail to add 3 on foo")
	}

	INCRBYCommand(NewCommand(fake, "incrby", "foo", "bar"))
	if fake.CompareErr(t, "value is not an integer or out of range") {
		t.Error("incrby: when c.Argv[2] is not an integer")
	}

	INCRBYCommand(NewCommand(fake, "incrby", "foo", fmt.Sprintf("%d", math.MaxInt64)))
	if fake.CompareErr(t, "increment or decrement would overflow") {
		t.Error("incrby: when overflow")
	}

	SETCommand(NewCommand(fake, "set", "foo", "bar"))
	fake.Flush()
	INCRCommand(NewCommand(fake, "incr", "foo"))
	if fake.CompareErr(t, "value is not an integer or out of range") {
		t.Error("incr: when foo is not an integer")
	}
}

func TestAPPEND(t *testing.T) {
	fake := NewFakeClient()

	APPENDCommand(NewCommand(fake, "append", "foo", "hello"))
	if fake.CompareInt64(t, 5) {
		t.Error("append: when foo not exists")
	}
	GETCommand(NewCommand(fake, "get", "foo"))
	if fake.CompareBulk(t, "hello") {
		t.Error("append: fail to set foo")
	}

	APPENDCommand(NewCommand(fake, "append", "foo", " world"))
	if fake.CompareInt64(t, 11) {
		t.Error("append: when foo exists")
	}
	GETCommand(NewCommand(fake, "get", "foo"))
	if fake.CompareBulk(t, "hello world") {
		t.Error("append: fail to update foo")
	}
}

func TestSTRLEN(t *testing.T) {
	fake := NewFakeClient()

	STRLENCommand(NewCommand(fake, "strlen", "foo"))
	if fake.CompareText(t, redigo.CZero) {
		t.Error("strlen: when foo not exists")
	}

	SETCommand(NewCommand(fake, "set", "foo", "bar"))
	fake.Flush()
	STRLENCommand(NewCommand(fake, "strlen", "foo"))
	if fake.CompareInt64(t, 3) {
		t.Error("strlen: when foo exists")
	}
}
