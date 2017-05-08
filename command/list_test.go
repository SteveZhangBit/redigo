package command

import (
	"testing"

	"github.com/SteveZhangBit/redigo"
)

func TestPUSH(t *testing.T) {
	fake := NewFakeClient()

	LPUSHCommand(NewCommand(fake, "lpush", "foo", "a", "b"))
	if fake.CompareText("2", t) {
		t.Error("lpush: when list not exists")
	}

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "c", "d"))
	if fake.CompareText("4", t) {
		t.Error("rpush: when list exists")
	}

	LPUSHXCommand(NewCommand(fake, "lpushx", "foo", "e"))
	if fake.CompareText("5", t) {
		t.Error("lpushx: when list exists")
	}

	RPUSHXCommand(NewCommand(fake, "rpushx", "bar", "a"))
	if fake.CompareText(redigo.CZero, t) {
		t.Error("rpushx: when list not exists")
	}
}

func TestLINSERT(t *testing.T) {
	fake := NewFakeClient()

	LPUSHCommand(NewCommand(fake, "lpush", "foo", "a"))
	fake.ReplyText = ""

	LINSERTCommand(NewCommand(fake, "linsert", "foo", "after", "a", "c"))
	if fake.CompareText("2", t) {
		t.Error("linsert: when after a and a exists")
	}

	LINSERTCommand(NewCommand(fake, "linsert", "foo", "before", "c", "b"))
	if fake.CompareText("3", t) {
		t.Error("linsert: when before c and c exists")
	}

	LINSERTCommand(NewCommand(fake, "linsert", "foo", "after", "d", "e"))
	if fake.CompareText(redigo.CNegOne, t) {
		t.Error("linsert: when after e but e not exists")
	}
}

func TestLLEN(t *testing.T) {
	fake := NewFakeClient()

	LPUSHCommand(NewCommand(fake, "lpush", "foo", "a", "b"))
	fake.ReplyText = ""
	LLENCommand(NewCommand(fake, "llen", "foo"))
	if fake.CompareText("2", t) {
		t.Error("llen: when lpush 2 elements to foo")
	}

	RPOPCommand(NewCommand(fake, "rpop", "foo"))
	fake.ReplyText = ""
	LLENCommand(NewCommand(fake, "llen", "foo"))
	if fake.CompareText("1", t) {
		t.Error("llen: when rpop from foo")
	}
}

func TestLINDEX(t *testing.T) {
	fake := NewFakeClient()

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "a", "b"))
	LINSERTCommand(NewCommand(fake, "linsert", "foo", "after", "b", "c"))
	fake.ReplyText = ""
	LINDEXCommand(NewCommand(fake, "lindex", "foo", "2"))
	if fake.CompareText("$1\r\nc\r\n", t) {
		t.Error("lindex: when rpush a b and linsert c after b")
	}

	LINDEXCommand(NewCommand(fake, "lindex", "foo", "4"))
	if fake.CompareText(redigo.NullBulk, t) {
		t.Error("lindex: when index = 4")
	}

	LINDEXCommand(NewCommand(fake, "lindex", "bar", "4"))
	if fake.CompareText(redigo.NullBulk, t) {
		t.Error("lindex: when list bar not exists")
	}
}

func TestLSET(t *testing.T) {
	fake := NewFakeClient()

	LSETCommand(NewCommand(fake, "lset", "foo", "2", "c"))
	if fake.CompareText(redigo.NoKeyErr, t) {
		t.Error("lset: when list foo not exists")
	}

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "a", "b", "c"))
	fake.ReplyText = ""
	LSETCommand(NewCommand(fake, "lset", "foo", "2", "d"))
	if fake.CompareText(redigo.OK, t) {
		t.Error("lset: when set foo[2] to d")
	}

	LSETCommand(NewCommand(fake, "lset", "foo", "5", "d"))
	if fake.CompareText(redigo.OutOfRangeErr, t) {
		t.Error("lset: when out of range")
	}
}

func TestPOP(t *testing.T) {
	fake := NewFakeClient()

	LPOPCommand(NewCommand(fake, "lpop", "foo"))
	if fake.CompareText(redigo.NullBulk, t) {
		t.Error("lpop: when foo not exists")
	}

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "a", "b", "c"))
	fake.ReplyText = ""
	LPOPCommand(NewCommand(fake, "lpop", "foo"))
	if fake.CompareText("$1\r\na\r\n", t) {
		t.Error("lpop: when foo has a b c")
	}
	RPOPCommand(NewCommand(fake, "rpop", "foo"))
	if fake.CompareText("$1\r\nc\r\n", t) {
		t.Error("rpop: when foo has b c")
	}

	RPOPCommand(NewCommand(fake, "rpop", "foo"))
	fake.ReplyText = ""
	LPOPCommand(NewCommand(fake, "lpop", "foo"))
	if fake.CompareText(redigo.NullBulk, t) {
		t.Error("lpop: when foo is empty")
	}
}

func TestLTRIM(t *testing.T) {
	fake := NewFakeClient()

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "a", "b", "c", "d"))
	LTRIMCommand(NewCommand(fake, "ltrim", "foo", "0", "2"))
	fake.ReplyText = ""
	LLENCommand(NewCommand(fake, "llen", "foo"))
	if fake.CompareText("3", t) {
		t.Error("ltrim: ltrim 0 2 when foo is abcd")
	}
	LINDEXCommand(NewCommand(fake, "lindex", "foo", "2"))
	if fake.CompareText("$1\r\nc\r\n", t) {
		t.Error("ltrim: ltrim 0 2 when foo is abcd")
	}

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "d"))
	LTRIMCommand(NewCommand(fake, "ltrim", "foo", "1", "-2"))
	fake.ReplyText = ""
	LLENCommand(NewCommand(fake, "llen", "foo"))
	if fake.CompareText("2", t) {
		t.Error("ltrim: ltrim 1 -2 when foo is abcd")
	}
	LINDEXCommand(NewCommand(fake, "lindex", "foo", "1"))
	if fake.CompareText("$1\r\nc\r\n", t) {
		t.Error("ltrim: ltrim 1 -2 when foo is abcd")
	}

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "d"))
	LPUSHCommand(NewCommand(fake, "lpush", "foo", "a"))
	LTRIMCommand(NewCommand(fake, "ltrim", "foo", "2", "-2"))
	fake.ReplyText = ""
	LLENCommand(NewCommand(fake, "llen", "foo"))
	if fake.CompareText("1", t) {
		t.Error("ltrim: ltrim 2 -2 when foo is abcd")
	}
	LINDEXCommand(NewCommand(fake, "lindex", "foo", "0"))
	if fake.CompareText("$1\r\nc\r\n", t) {
		t.Error("ltrim: ltrim 2 -2 when foo is abcd")
	}

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "d"))
	LPUSHCommand(NewCommand(fake, "lpush", "foo", "a", "b"))
	LTRIMCommand(NewCommand(fake, "ltrim", "foo", "5", "0"))
	fake.ReplyText = ""
	LLENCommand(NewCommand(fake, "llen", "foo"))
	if fake.CompareText(redigo.CZero, t) {
		t.Error("ltrim: ltrim 5 0 when foo is abcd")
	}
	LINDEXCommand(NewCommand(fake, "lindex", "foo", "0"))
	if fake.CompareText(redigo.NullBulk, t) {
		t.Error("ltrim: ltrim 5 0 when foo is abcd")
	}
}

func TestLREM(t *testing.T) {
	fake := NewFakeClient()

	RPUSHCommand(NewCommand(fake, "rpush", "foo", "a", "b", "a", "c", "c", "e", "c"))
	fake.ReplyText = ""
	LREMCommand(NewCommand(fake, "lrem", "foo", "2", "a"))
	if fake.CompareText("2", t) {
		t.Error("lrem: lrem 2 a when foo is abaccec")
	}
	LREMCommand(NewCommand(fake, "lrem", "foo", "-2", "c"))
	if fake.CompareText("2", t) {
		t.Error("lrem: lrem -2 c when foo is bccec")
	}

	LREMCommand(NewCommand(fake, "lrem", "foo", "0", "d"))
	if fake.CompareText("0", t) {
		t.Error("lrem: lrem 0 d when foo is bce")
	}

	LREMCommand(NewCommand(fake, "lrem", "foo", "0", "b"))
	if fake.CompareText("1", t) {
		t.Error("lrem: lrem 0 b when foo is bce")
	}
	LINDEXCommand(NewCommand(fake, "lindex", "foo", "0"))
	if fake.CompareText("$1\r\nc\r\n", t) {
		t.Error("lrem: lrem 0 b when foo is ce")
	}
}
