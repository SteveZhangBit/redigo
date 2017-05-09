package command

import (
	"testing"

	"github.com/SteveZhangBit/redigo"
)

func TestSADD(t *testing.T) {
	fake := NewFakeClient()

	SADDCommand(NewCommand(fake, "sadd", "s", "a"))
	if fake.CompareInt64(t, 1) {
		t.Error("sadd: when s and a not exists")
	}

	SADDCommand(NewCommand(fake, "sadd", "s", "a", "b"))
	if fake.CompareInt64(t, 1) {
		t.Error("sadd: when s and a exists, b not exists")
	}
}

func TestSREM(t *testing.T) {
	fake := NewFakeClient()

	SREMCommand(NewCommand(fake, "srem", "s", "a"))
	if fake.CompareText(t, redigo.CZero) {
		t.Error("srem: when s and a not exists")
	}

	SADDCommand(NewCommand(fake, "sadd", "s", "a", "b"))
	fake.Text = ""
	SREMCommand(NewCommand(fake, "srem", "s", "a"))
	if fake.CompareInt64(t, 1) {
		t.Error("srem: when s and a exists")
	}

	SREMCommand(NewCommand(fake, "srem", "s", "a", "b"))
	if fake.CompareInt64(t, 1) {
		t.Error("srem: when s and b exists, but a not exists")
	}
}

func TestSISMEMBER(t *testing.T) {
	fake := NewFakeClient()

	SISMEMBERCommand(NewCommand(fake, "sismember", "s", "a"))
	if fake.CompareText(t, redigo.CZero) {
		t.Error("sismember: when s not exists")
	}

	SADDCommand(NewCommand(fake, "sadd", "s", "a", "b"))
	fake.Text = ""
	SISMEMBERCommand(NewCommand(fake, "sismember", "s", "a"))
	if fake.CompareText(t, redigo.COne) {
		t.Error("sismember: when s and a exists")
	}
	SISMEMBERCommand(NewCommand(fake, "sismember", "s", "c"))
	if fake.CompareText(t, redigo.CZero) {
		t.Error("sismember: when c not exists")
	}
}

func TestSCARD(t *testing.T) {
	fake := NewFakeClient()

	SCARDCommand(NewCommand(fake, "scard", "s"))
	if fake.CompareText(t, redigo.CZero) {
		t.Error("scard: when s not exists")
	}

	SADDCommand(NewCommand(fake, "sadd", "s", "a", "b"))
	fake.Text = ""
	SCARDCommand(NewCommand(fake, "scard", "s"))
	if fake.CompareInt64(t, 2) {
		t.Error("scard: when s has a and b")
	}
}

func TestSPOP(t *testing.T) {
	fake := NewFakeClient()

	SPOPCommand(NewCommand(fake, "spop", "s"))
	if fake.CompareText(t, redigo.CZero) {
		t.Error("spop: when s not exists")
	}

	SADDCommand(NewCommand(fake, "sadd", "s", "a", "b", "c"))
	fake.Text = ""
	SPOPCommand(NewCommand(fake, "spop", "s"))
	t.Logf("%q", fake.Text)
	fake.Text = ""
	SCARDCommand(NewCommand(fake, "scard", "s"))
	if fake.CompareInt64(t, 2) {
		t.Error("spop: when s had abc and poped one")
	}
}
