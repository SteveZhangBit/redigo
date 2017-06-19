package util

import "testing"

func TestStringMatchPattern(t *testing.T) {
	if !StringMatchPattern("*", "foo", false) {
		t.Error("*", "foo")
	}
	if !StringMatchPattern("?oo", "foo", false) {
		t.Error("?oo", "foo")
	}
	if StringMatchPattern("?oo", "fo", false) {
		t.Error("?oo", "fo")
	}
	if !StringMatchPattern("[a-z]?o", "foo", false) {
		t.Error("[a-z]?o", "foo")
	}
	if StringMatchPattern("[a-z]?o", "_o", false) {
		t.Error("[a-z]?o", "_o")
	}
}
