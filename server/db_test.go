package server

import (
	"testing"
)

func TestDBAdd(t *testing.T) {
	db := NewDB()
	db.Add("foo", "bar")
	if db.LookupKey("foo") != "bar" {
		t.Error("Add: when not exists failed")
	}

	defer func() {
		if p := recover(); p == nil {
			t.Error("Add: when exists failed")
		}
	}()
	db.Add("foo", "barrr")
}

func TestDBLookupRead(t *testing.T) {
	db := NewDB()
	db.Add("foo", "bar")
	if db.LookupKeyRead("foo") != "bar" || db.KeyspaceHits != 1 {
		t.Error("LookupKeyRead: when exists failed")
	}
	if db.LookupKeyRead("bar") != nil || db.KeyspaceMisses != 1 {
		t.Error("LookupKeyRead: when not exists failed")
	}
}

func TestDBUpdate(t *testing.T) {
	db := NewDB()
	db.Add("foo", "bar")
	db.Update("foo", "barrr")
	if db.LookupKey("foo") != "barrr" {
		t.Error("Update: when exists failed")
	}

	defer func() {
		if p := recover(); p == nil {
			t.Error("Update: when not exists failed")
		}
	}()
	db.Update("bar", "foo")
}

func TestDBDelete(t *testing.T) {
	db := NewDB()
	db.Add("foo", "bar")
	if !db.Delete("foo") {
		t.Error("Delete: when exists failed")
	}
	if db.Delete("bar") {
		t.Error("Delete: when not exists failed")
	}
}

func TestDBSetKeyPersist(t *testing.T) {
	db := NewDB()
	db.SetKeyPersist("foo", "bar")
	if db.LookupKey("foo") != "bar" {
		t.Error("SetKeyPersist: when not exists failed")
	}
	db.SetKeyPersist("foo", "barrr")
	if db.LookupKey("foo") != "barrr" {
		t.Error("SetKeyPersist: when exists failed")
	}
}

func TestRandomKey(t *testing.T) {
	db := NewDB()
	db.Add("foo", "bar")
	db.Add("fooo", "bar")
	db.Add("foooo", "bar")
	db.Add("fooooo", "bar")
	db.Add("foooooo", "bar")
	t.Log(db.RandomKey())
	t.Log(db.RandomKey())
	t.Log(db.RandomKey())
}
