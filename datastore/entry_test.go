package datastore

import (
	"bufio"
	"bytes"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	e := entry{key: "key", value: "value"}
	encoded := e.Encode()

	var decoded entry
	decoded.Decode(encoded)

	if decoded.key != "key" {
		t.Error("incorrect key")
	}
	if decoded.value != "value" {
		t.Error("incorrect value")
	}
}

func TestReadValue(t *testing.T) {
	e := entry{key: "key", value: "value"}
	data := e.Encode()

	v, err := readValue(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if v != "value" {
		t.Errorf("incorrect value [%s]", v)
	}
}
