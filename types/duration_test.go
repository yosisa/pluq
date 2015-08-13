package types

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	d, err := ParseDuration("1s")
	if err != nil || d != Duration(time.Second) {
		t.Fatalf("Couldn't parse '1s'")
	}
}

func TestDurationString(t *testing.T) {
	if s := Duration(time.Second).String(); s != "1s" {
		t.Fatalf("Expected 1s but %s", s)
	}
}

func TestDurationToJSON(t *testing.T) {
	d := Duration(time.Second)
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, []byte(`"1s"`)) {
		t.Fatalf(`Expected "1s" but %s`, b)
	}
	var got Duration
	if err = json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if d != got {
		t.Fatalf("Expected %v but %v", d, got)
	}
}
