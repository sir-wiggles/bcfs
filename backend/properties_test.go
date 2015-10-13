package backend

import (
	"testing"
)


func Test_StringKey(t *testing.T) {
	props := properties{
		"str": "string a",
	}

	// + test
	v, e := props.StringKey("str")
	if e != nil {
		t.Error(e.Error())
	}
	if v != "string a" {
		t.Error("string does not match expected")
	}

	// - test
	v, e = props.StringKey("invalid")
	if e == nil {
		t.Error()
	}
	if v != "" {
		t.Error("invalid key should have returned an empty string")
	}
}