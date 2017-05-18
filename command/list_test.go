package command

import (
	"testing"

	"github.com/SteveZhangBit/redigo"
)

func TestPUSH(t *testing.T) {
	fake := NewFakeClient()

	LPUSHCommand(NewCommand(fake, "lpush", "foo", "a", "b"))
	if fake.CompareInt64(t, 2) {
		t.Error("lpush: when list not exists")
	}

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "c", "d"))
	if fake.CompareInt64(t, 4) {
		t.Error("rpush: when list exists")
	}

	LPUSHXCommand(NewCommand(fake, "lpushx", "foo", "e"))
	if fake.CompareInt64(t, 5) {
		t.Error("lpushx: when list exists")
	}

	RPUSHXCommand(NewCommand(fake, "rpushx", "bar", "a"))
	if fake.CompareText(t, redigo.CZero) {
		t.Error("rpushx: when list not exists")
	}
}

func TestLINSERT(t *testing.T) {
	fake := NewFakeClient()

	LPUSHCommand(NewCommand(fake, "lpush", "foo", "a"))
	fake.Flush()

	LINSERTCommand(NewCommand(fake, "linsert", "foo", "after", "a", "c"))
	if fake.CompareInt64(t, 2) {
		t.Error("linsert: when after a and a exists")
	}

	LINSERTCommand(NewCommand(fake, "linsert", "foo", "before", "c", "b"))
	if fake.CompareInt64(t, 3) {
		t.Error("linsert: when before c and c exists")
	}

	LINSERTCommand(NewCommand(fake, "linsert", "foo", "after", "d", "e"))
	if fake.CompareText(t, redigo.CNegOne) {
		t.Error("linsert: when after e but e not exists")
	}
}

func TestLLEN(t *testing.T) {
	fake := NewFakeClient()

	LPUSHCommand(NewCommand(fake, "lpush", "foo", "a", "b"))
	fake.Flush()
	LLENCommand(NewCommand(fake, "llen", "foo"))
	if fake.CompareInt64(t, 2) {
		t.Error("llen: when lpush 2 elements to foo")
	}

	RPOPCommand(NewCommand(fake, "rpop", "foo"))
	fake.Flush()
	LLENCommand(NewCommand(fake, "llen", "foo"))
	if fake.CompareInt64(t, 1) {
		t.Error("llen: when rpop from foo")
	}
}

func TestLINDEX(t *testing.T) {
	fake := NewFakeClient()

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "a", "b"))
	LINSERTCommand(NewCommand(fake, "linsert", "foo", "after", "b", "c"))
	fake.Flush()
	LINDEXCommand(NewCommand(fake, "lindex", "foo", "2"))
	if fake.CompareBulk(t, "c") {
		t.Error("lindex: when rpush a b and linsert c after b")
	}

	LINDEXCommand(NewCommand(fake, "lindex", "foo", "4"))
	if fake.CompareText(t, redigo.NullBulk) {
		t.Error("lindex: when index = 4")
	}

	LINDEXCommand(NewCommand(fake, "lindex", "bar", "4"))
	if fake.CompareText(t, redigo.NullBulk) {
		t.Error("lindex: when list bar not exists")
	}
}

func TestLSET(t *testing.T) {
	fake := NewFakeClient()

	LSETCommand(NewCommand(fake, "lset", "foo", "2", "c"))
	if fake.CompareText(t, redigo.NoKeyErr) {
		t.Error("lset: when list foo not exists")
	}

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "a", "b", "c"))
	fake.Flush()
	LSETCommand(NewCommand(fake, "lset", "foo", "2", "d"))
	if fake.CompareText(t, redigo.OK) {
		t.Error("lset: when set foo[2] to d")
	}

	LSETCommand(NewCommand(fake, "lset", "foo", "5", "d"))
	if fake.CompareText(t, redigo.OutOfRangeErr) {
		t.Error("lset: when out of range")
	}
}

func TestPOP(t *testing.T) {
	fake := NewFakeClient()

	LPOPCommand(NewCommand(fake, "lpop", "foo"))
	if fake.CompareText(t, redigo.NullBulk) {
		t.Error("lpop: when foo not exists")
	}

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "a", "b", "c"))
	fake.Flush()
	LPOPCommand(NewCommand(fake, "lpop", "foo"))
	if fake.CompareBulk(t, "a") {
		t.Error("lpop: when foo has a b c")
	}
	RPOPCommand(NewCommand(fake, "rpop", "foo"))
	if fake.CompareBulk(t, "c") {
		t.Error("rpop: when foo has b c")
	}

	RPOPCommand(NewCommand(fake, "rpop", "foo"))
	fake.Flush()
	LPOPCommand(NewCommand(fake, "lpop", "foo"))
	if fake.CompareText(t, redigo.NullBulk) {
		t.Error("lpop: when foo is empty")
	}
}

func TestLTRIM(t *testing.T) {
	fake := NewFakeClient()

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "a", "b", "c", "d"))
	LTRIMCommand(NewCommand(fake, "ltrim", "foo", "0", "2"))
	fake.Flush()
	LLENCommand(NewCommand(fake, "llen", "foo"))
	if fake.CompareInt64(t, 3) {
		t.Error("ltrim: ltrim 0 2 when foo is abcd")
	}
	LINDEXCommand(NewCommand(fake, "lindex", "foo", "2"))
	if fake.CompareBulk(t, "c") {
		t.Error("ltrim: ltrim 0 2 when foo is abcd")
	}

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "d"))
	LTRIMCommand(NewCommand(fake, "ltrim", "foo", "1", "-2"))
	fake.Flush()
	LLENCommand(NewCommand(fake, "llen", "foo"))
	if fake.CompareInt64(t, 2) {
		t.Error("ltrim: ltrim 1 -2 when foo is abcd")
	}
	LINDEXCommand(NewCommand(fake, "lindex", "foo", "1"))
	if fake.CompareBulk(t, "c") {
		t.Error("ltrim: ltrim 1 -2 when foo is abcd")
	}

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "d"))
	LPUSHCommand(NewCommand(fake, "lpush", "foo", "a"))
	LTRIMCommand(NewCommand(fake, "ltrim", "foo", "2", "-2"))
	fake.Flush()
	LLENCommand(NewCommand(fake, "llen", "foo"))
	if fake.CompareInt64(t, 1) {
		t.Error("ltrim: ltrim 2 -2 when foo is abcd")
	}
	LINDEXCommand(NewCommand(fake, "lindex", "foo", "0"))
	if fake.CompareBulk(t, "c") {
		t.Error("ltrim: ltrim 2 -2 when foo is abcd")
	}

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "d"))
	LPUSHCommand(NewCommand(fake, "lpush", "foo", "a", "b"))
	LTRIMCommand(NewCommand(fake, "ltrim", "foo", "5", "0"))
	fake.Flush()
	LLENCommand(NewCommand(fake, "llen", "foo"))
	if fake.CompareText(t, redigo.CZero) {
		t.Error("ltrim: ltrim 5 0 when foo is abcd")
	}
	LINDEXCommand(NewCommand(fake, "lindex", "foo", "0"))
	if fake.CompareText(t, redigo.NullBulk) {
		t.Error("ltrim: ltrim 5 0 when foo is abcd")
	}
}

func TestLREM(t *testing.T) {
	fake := NewFakeClient()

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "a", "b", "a", "c", "c", "e", "c"))
	fake.Flush()
	LREMCommand(NewCommand(fake, "lrem", "foo", "2", "a"))
	if fake.CompareInt64(t, 2) {
		t.Error("lrem: lrem 2 a when foo is abaccec")
	}
	LREMCommand(NewCommand(fake, "lrem", "foo", "-2", "c"))
	if fake.CompareInt64(t, 2) {
		t.Error("lrem: lrem -2 c when foo is bccec")
	}

	LREMCommand(NewCommand(fake, "lrem", "foo", "0", "d"))
	if fake.CompareInt64(t, 0) {
		t.Error("lrem: lrem 0 d when foo is bce")
	}

	LREMCommand(NewCommand(fake, "lrem", "foo", "0", "b"))
	if fake.CompareInt64(t, 1) {
		t.Error("lrem: lrem 0 b when foo is bce")
	}
	LINDEXCommand(NewCommand(fake, "lindex", "foo", "0"))
	if fake.CompareBulk(t, "c") {
		t.Error("lrem: lrem 0 b when foo is ce")
	}
}

func TestLRANGE(t *testing.T) {
	fake := NewFakeClient()

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "a", "b", "c", "d", "e", "f"))
	fake.Flush()
	LRANGECommand(NewCommand(fake, "lrange", "foo", "1", "3"))
	if fake.CompareMultiBulk(t, "b", "c", "d") {
		t.Error("lrange: range between 1 3 and foo is abcdef")
	}

	LRANGECommand(NewCommand(fake, "lrange", "foo", "2", "-3"))
	if fake.CompareMultiBulk(t, "c", "d") {
		t.Error("lrange: range between 2 -3 and foo is abcdef")
	}

	LRANGECommand(NewCommand(fake, "lrange", "foo", "-2", "-3"))
	if fake.CompareText(t, redigo.EmptyMultiBulk) {
		t.Error("lrange: range between -2 -3 and foo is abcdef")
	}

	LRANGECommand(NewCommand(fake, "lrange", "foo", "4", "9"))
	if fake.CompareMultiBulk(t, "e", "f") {
		t.Error("lrange: range between 4 9 and foo is abcdef")
	}
}
