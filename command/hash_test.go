package command

import (
	"fmt"
	"math"
	"testing"

	"github.com/SteveZhangBit/redigo"
)

func TestHSETandHGET(t *testing.T) {
	fake := NewFakeClient()

	HGETCommand(NewCommand(fake, "hget", "s", "foo"))
	if fake.CompareText(redigo.NullBulk, t) {
		t.Error("hget: when s not exists")
	}

	HSETCommand(NewCommand(fake, "hset", "s", "foo", "bar"))
	if fake.CompareText(redigo.COne, t) {
		t.Error("hset: when s not exists and key foo not exists")
	}

	HGETCommand(NewCommand(fake, "hget", "s", "foo"))
	if fake.CompareText("$3\r\nbar\r\n", t) {
		t.Error("hget: when s and key foo exists")
	}

	HGETCommand(NewCommand(fake, "hget", "s", "bar"))
	if fake.CompareText(redigo.NullBulk, t) {
		t.Error("hget: when s exists but key bar not exists")
	}

	HSETCommand(NewCommand(fake, "hset", "s", "foo", "barr"))
	if fake.CompareText(redigo.CZero, t) {
		t.Error("hset: when s exists and key foo exists")
	}

	HGETCommand(NewCommand(fake, "hget", "s", "foo"))
	if fake.CompareText("$4\r\nbarr\r\n", t) {
		t.Error("hget: after update key foo")
	}

	SETCommand(NewCommand(fake, "set", "s2", "bar"))
	fake.ReplyText = ""
	HSETCommand(NewCommand(fake, "hset", "s2", "foo", "bar"))
	if fake.CompareText(redigo.WrongTypeErr, t) {
		t.Error("hset: when s exists but not a set")
	}

	HGETCommand(NewCommand(fake, "hget", "s2", "bar"))
	if fake.CompareText(redigo.WrongTypeErr, t) {
		t.Error("hget: when s exists but not a set")
	}
}

func TestHSETNX(t *testing.T) {
	fake := NewFakeClient()

	HSETNXCommand(NewCommand(fake, "hsetnx", "s", "foo", "bar"))
	if fake.CompareText(redigo.COne, t) {
		t.Error("hsetnx: when s not exists and key foo not exists")
	}

	HSETNXCommand(NewCommand(fake, "hsetnx", "s", "foo", "barr"))
	if fake.CompareText(redigo.CZero, t) {
		t.Error("hsetnx: when s exists and key foo exists")
	}
}

func TestHMSET(t *testing.T) {
	fake := NewFakeClient()

	HMSETCommand(NewCommand(fake, "hmset", "s", "foo", "bar", "fooo"))
	if fake.CompareText("-ERR wrong number of arguments for HMSET", t) {
		t.Error("hmset: when c.Argc%2 == 1")
	}

	HMSETCommand(NewCommand(fake, "hmset", "s", "foo", "bar", "fooo", "barrr"))
	if fake.CompareText(redigo.OK, t) {
		t.Error("hmset: when c.Argc%2 == 0")
	}
	HGETCommand(NewCommand(fake, "hget", "s", "fooo"))
	if fake.CompareText("$5\r\nbarrr\r\n", t) {
		t.Error("hmset: when set foo bar and fooo barrr")
	}
}

func TestHINCRBY(t *testing.T) {
	fake := NewFakeClient()

	HINCRBYCommand(NewCommand(fake, "hincrby", "s", "foo", "1"))
	if fake.CompareText("1", t) {
		t.Error("hincrby: when s and key foo not exist")
	}

	HINCRBYCommand(NewCommand(fake, "hincrby", "s", "foo", "4"))
	if fake.CompareText("5", t) {
		t.Error("hincrby: when s and key foo exist")
	}

	HINCRBYCommand(NewCommand(fake, "hincrby", "s", "foo", fmt.Sprintf("%d", math.MaxInt64)))
	if fake.CompareText("-ERR increment or decrement would overflow", t) {
		t.Error("hincrby: when integer overflow")
	}

	HSETCommand(NewCommand(fake, "hset", "s", "foo", "bar"))
	fake.ReplyText = ""
	HINCRBYCommand(NewCommand(fake, "hincrby", "s", "foo", "4"))
	if fake.CompareText("-ERR hash value is not an integer", t) {
		t.Error("hincry: when s and key foo exist, but not an integer")
	}
}

func TestHINCRBYFLOAT(t *testing.T) {
	fake := NewFakeClient()

	var f string
	HINCRBYFLOATCommand(NewCommand(fake, "hincrbyfloat", "s", "foo", "1.5"))
	f = fmt.Sprintf("%.17f", 1.5)
	if fake.CompareText(fmt.Sprintf("$%d\r\n%s\r\n", len(f), f), t) {
		t.Error("hincrbyfloat: when s and key foo not exist")
	}

	HINCRBYFLOATCommand(NewCommand(fake, "hincrbyfloat", "s", "foo", "2.3"))
	f = fmt.Sprintf("%.17f", 1.5+2.3)
	if fake.CompareText(fmt.Sprintf("$%d\r\n%s\r\n", len(f), f), t) {
		t.Error("hincrbyfloat: when s and key foo exist")
	}

	HSETCommand(NewCommand(fake, "hset", "s", "foo", "bar"))
	fake.ReplyText = ""
	HINCRBYFLOATCommand(NewCommand(fake, "hincrbyfloat", "s", "foo", "3.2"))
	if fake.CompareText("-ERR hash value is not a valid float", t) {
		t.Error("hincry: when s and key foo exist, but not an integer")
	}
}

func TestHMGET(t *testing.T) {
	fake := NewFakeClient()

	HMGETCommand(NewCommand(fake, "hmget", "s", "foo", "fooo"))
	if fake.CompareText(fmt.Sprintf("*2\r\n%s%s", redigo.NullBulk, redigo.NullBulk), t) {
		t.Error("hmget: when s not exists")
	}

	HMSETCommand(NewCommand(fake, "hmset", "s", "foo", "bar"))
	fake.ReplyText = ""
	HMGETCommand(NewCommand(fake, "hmget", "s", "foo", "fooo"))
	if fake.CompareText(fmt.Sprintf("*2\r\n$3\r\nbar\r\n%s", redigo.NullBulk), t) {
		t.Error("hmget: when key foo exists but fooo not exist")
	}

	SETCommand(NewCommand(fake, "set", "s", "foo"))
	fake.ReplyText = ""
	HMGETCommand(NewCommand(fake, "hmget", "s", "foo", "fooo"))
	if fake.CompareText(redigo.WrongTypeErr, t) {
		t.Error("hmget: when s is not a set")
	}
}

func TestHDEL(t *testing.T) {
	fake := NewFakeClient()

	HDELCommand(NewCommand(fake, "hdel", "s", "foo"))
	if fake.CompareText(redigo.CZero, t) {
		t.Error("hdel: when s not exists")
	}

	HMSETCommand(NewCommand(fake, "hmset", "s", "foo", "bar", "fooo", "barr"))
	fake.ReplyText = ""
	HDELCommand(NewCommand(fake, "hdel", "s", "foo", "fooo", "foooo"))
	if fake.CompareText("2", t) {
		t.Error("hdel: when foo, fooo exist, but foooo not exists")
	}

	SETCommand(NewCommand(fake, "set", "s", "foo"))
	fake.ReplyText = ""
	HDELCommand(NewCommand(fake, "hdel", "s", "foo", "fooo", "foooo"))
	if fake.CompareText(redigo.WrongTypeErr, t) {
		t.Error("hdel: when s is not hset")
	}
}

func TestHLEN(t *testing.T) {
	fake := NewFakeClient()

	HMSETCommand(NewCommand(fake, "hmset", "s", "foo", "bar", "fooo", "barr"))
	fake.ReplyText = ""
	HLENCommand(NewCommand(fake, "hlen", "s"))
	if fake.CompareText("2", t) {
		t.Error("hlen: when s has 2 keys")
	}
}

func TestHEXISTS(t *testing.T) {
	fake := NewFakeClient()

	HSETCommand(NewCommand(fake, "hset", "s", "foo", "bar"))
	fake.ReplyText = ""
	HEXISTSCommand(NewCommand(fake, "hexists", "s", "foo"))
	if fake.CompareText(redigo.COne, t) {
		t.Error("hexists: when foo exists")
	}

	HEXISTSCommand(NewCommand(fake, "hexists", "s", "fooo"))
	if fake.CompareText(redigo.CZero, t) {
		t.Error("hexists: when fooo not exists")
	}
}

func TestGETALL(t *testing.T) {
	fake := NewFakeClient()

	HMSETCommand(NewCommand(fake, "hmset", "s", "foo", "bar", "fooo", "barr"))
	fake.ReplyText = ""
	HKEYSCommand(NewCommand(fake, "hkeys", "s"))
	t.Logf("%q", fake.ReplyText)
	fake.ReplyText = ""

	HVALSCommand(NewCommand(fake, "hvals", "s"))
	t.Logf("%q", fake.ReplyText)
	fake.ReplyText = ""

	HGETALLCommand(NewCommand(fake, "hgetall", "s"))
	t.Logf("%q", fake.ReplyText)
	fake.ReplyText = ""
}
