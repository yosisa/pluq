package types

import (
	"errors"
	"fmt"
	"time"
)

type Duration time.Duration

func (z Duration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%v"`, time.Duration(z))), nil
}

func (z *Duration) UnmarshalJSON(b []byte) error {
	tail := len(b) - 1
	if tail < 2 || b[0] != '"' || b[tail] != '"' {
		return errors.New("Duration must be string")
	}
	d, err := time.ParseDuration(string(b[1:tail]))
	if err == nil {
		*z = Duration(d)
	}
	return err
}

func (z Duration) String() string {
	return time.Duration(z).String()
}

func ParseDuration(s string) (Duration, error) {
	d, err := time.ParseDuration(s)
	return Duration(d), err
}
