package command

import (
	"fmt"
	"testing"

	"github.com/SteveZhangBit/redigo"
)

func TestZADD(t *testing.T) {
	fake := NewFakeClient()

	ZADDCommand(NewCommand(fake, "zadd", "z", "1", "a"))
	if fake.CompareInt64(t, 1) {
		t.Error("zadd: when z and 1-a not exists")
	}

	ZADDCommand(NewCommand(fake, "zadd", "z", "1", "a", "1", "b"))
	if fake.CompareInt64(t, 1) {
		t.Error("zadd: when 1-a exists but 1-b not exists")
	}

	ZADDCommand(NewCommand(fake, "zadd", "z", "2", "a"))
	if fake.CompareInt64(t, 1) {
		t.Error("zadd: when intend to update 1-a to 2-a")
	}
}

func TestZREM(t *testing.T) {
	fake := NewFakeClient()

	ZADDCommand(NewCommand(fake, "zadd", "z", "1", "a", "1", "b"))
	fake.Flush()
	ZREMCommand(NewCommand(fake, "zrem", "z", "a"))
	if fake.CompareInt64(t, 1) {
		t.Error("zrem: failed to remove a when z has 1-a and 1-b")
	}

	ZREMCommand(NewCommand(fake, "zrem", "z", "c"))
	if fake.CompareInt64(t, 0) {
		t.Error("zrem: failed to remove c when z has 1-b")
	}
}

func TestZRANGE(t *testing.T) {
	fake := NewFakeClient()

	ZADDCommand(NewCommand(fake, "zadd", "z", "1", "a", "1", "b", "2", "c", "1.5", "d"))
	fake.Flush()
	ZRANGECommand(NewCommand(fake, "zrange", "z", "0", "10"))
	if fake.CompareMultiBulk(t, "a", "b", "d", "c") {
		t.Error("zrange: range from 0~10 when z has 1-a, 1-b, 1.5-d, 2-c")
	}

	ZRANGECommand(NewCommand(fake, "zrange", "z", "1", "-2"))
	if fake.CompareMultiBulk(t, "b", "d") {
		t.Error("zrange: range from 1~-2 when z has 1-a, 1-b, 1.5-d, 2-c")
	}

	ZRANGECommand(NewCommand(fake, "zrange", "z", "5", "0"))
	if fake.CompareText(t, redigo.EmptyMultiBulk) {
		t.Error("zrange: range from 5~0")
	}

	ZRANGECommand(NewCommand(fake, "zrange", "z", "-1", "-3"))
	if fake.CompareText(t, redigo.EmptyMultiBulk) {
		t.Error("zrange: range from -1~-3")
	}

	ZREVRANGECommand(NewCommand(fake, "zrange", "z", "0", "-1", "withscores"))
	f1 := fmt.Sprintf("%.17g", 1.0)
	f1_5 := fmt.Sprintf("%.17g", 1.5)
	f2 := fmt.Sprintf("%.17g", 2.0)
	if fake.CompareMultiBulk(t, "c", f2, "d", f1_5, "b", f1, "a", f1) {
		t.Error("zrevrange: revrange from 0~-1 when z has 1-a, 1-b, 1.5-d, 2-c")
	}
}

func TestZCARD(t *testing.T) {
	fake := NewFakeClient()

	ZADDCommand(NewCommand(fake, "zadd", "z", "1", "a", "1", "b", "2", "c", "1.5", "d"))
	fake.Flush()
	ZCARDCommand(NewCommand(fake, "zcard", "z"))
	if fake.CompareInt64(t, 4) {
		t.Error("zcard: when z has 1-a, 1-b, 1.5-d, 2-c")
	}
}

func TestZSCORE(t *testing.T) {
	fake := NewFakeClient()

	ZADDCommand(NewCommand(fake, "zadd", "z", "1", "a", "1", "b", "2", "c", "1.5", "d"))
	fake.Flush()

	ZSCORECommand(NewCommand(fake, "zscore", "z", "c"))
	if fake.CompareFloat64(t, 2.0) {
		t.Error("zscore: when z has 2-c")
	}

	ZSCORECommand(NewCommand(fake, "zscore", "z", "e"))
	if fake.CompareText(t, redigo.NullBulk) {
		t.Error("zscore: when e not exists in z")
	}
}
