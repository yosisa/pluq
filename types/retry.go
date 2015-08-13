package types

import (
	"bytes"
	"strconv"
)

type Retry int

const RetryNoLimit Retry = -9999

func (z Retry) MarshalJSON() ([]byte, error) {
	if z == RetryNoLimit {
		return []byte(`"nolimit"`), nil
	}
	return []byte(strconv.Itoa(int(z))), nil
}

func (z *Retry) UnmarshalJSON(b []byte) error {
	if bytes.Equal(b, []byte(`"nolimit"`)) {
		*z = RetryNoLimit
		return nil
	}
	return z.atoi(string(b))
}

func (z *Retry) atoi(s string) error {
	n, err := strconv.Atoi(s)
	if err == nil {
		*z = Retry(n)
	}
	return err
}

func (z Retry) String() string {
	if z == RetryNoLimit {
		return "nolimit"
	}
	return strconv.Itoa(int(z))
}

func (z *Retry) Incr() {
	if *z != RetryNoLimit {
		*z += 1
	}
}

func (z *Retry) Decr() {
	if *z != RetryNoLimit {
		*z -= 1
	}
}

func (z Retry) IsValid() bool {
	return z == RetryNoLimit || z > 0
}

func ParseRetry(s string) (Retry, error) {
	var z Retry
	if s == "nolimit" {
		z = RetryNoLimit
		return z, nil
	}
	err := z.atoi(s)
	return z, err
}
