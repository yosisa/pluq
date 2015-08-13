package types

import (
	"encoding/json"
	"testing"
)

func TestParseRetry(t *testing.T) {
	r, err := ParseRetry("nolimit")
	if err != nil || r != RetryNoLimit {
		t.Fatalf("Couldn't parse 'nolimit'")
	}

	r, err = ParseRetry("1")
	if err != nil || r != 1 {
		t.Fatalf("Couldn't parse '1'")
	}

	_, err = ParseRetry("a")
	if err == nil {
		t.Fatalf("Error expected")
	}
}

func TestRetryString(t *testing.T) {
	if s := RetryNoLimit.String(); s != "nolimit" {
		t.Fatalf("Expected nolimit but %s", s)
	}
	if s := Retry(1).String(); s != "1" {
		t.Fatalf("Expected 1 but %s", s)
	}
}

func TestRetryToJSON(t *testing.T) {
	for _, r := range []Retry{0, 1, RetryNoLimit} {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		var got Retry
		err = json.Unmarshal(b, &got)
		if err != nil {
			t.Fatal(err)
		}
		if got != r {
			t.Fatalf("Expected %v but %v", r, got)
		}
	}
}

func TestRetryIncrDecr(t *testing.T) {
	r := Retry(1)
	r.Incr()
	if int(r) != 2 {
		t.Fatalf("Expected 2 but %v", r)
	}
	r.Decr()
	if int(r) != 1 {
		t.Fatalf("Expected 1 but %v", r)
	}

	r = RetryNoLimit
	r.Incr()
	if r != RetryNoLimit {
		t.Fatalf("Expected nolimit but %v", r)
	}
	r.Decr()
	if r != RetryNoLimit {
		t.Fatalf("Expected nolimit but %v", r)
	}
}

func TestRetryValidity(t *testing.T) {
	if r := RetryNoLimit; !r.IsValid() {
		t.Fatalf("%v is invalid", r)
	}
	if r := Retry(1); !r.IsValid() {
		t.Fatalf("%v is invalid", r)
	}
}
