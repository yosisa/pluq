// Package uid provides 64-bit integer id generator inspired by twitter's
// snowflake. A generated id is unique and k-sorted (time-ordered lexically) in
// a distributed system without any coordination. The id consists of four parts:
//
// - 1 bit: sign flag but not used
// - 41 bits: the timestamp in milliseconds since a custom epoch
// - 10 bits: the machine id
// - 12 bits: the sequence number

package uid

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/speps/go-hashids"
)

const (
	machineMask = 0x03ff
	seqMask     = 0x0fff
)

var (
	ErrClock     = errors.New("Error clock moved backwards")
	ErrInvalidID = errors.New("Error invalid hashid")

	epoch int64
	salt  string
	clock = func() int64 {
		return time.Now().UnixNano()
	}
	hashid *hashids.HashID
)

func SetEpoch(t time.Time) {
	epoch = t.UnixNano()
}

func SetSalt(s string) {
	salt = s
	hd := hashids.NewData()
	hd.Salt = salt
	hashid = hashids.NewWithData(hd)
}

type ID int64

func FromHashID(s string) (ID, error) {
	vals := hashid.DecodeInt64(s)
	if len(vals) != 1 {
		return 0, ErrInvalidID
	}
	return ID(vals[0]), nil
}

func (n ID) Bytes() []byte {
	b := make([]byte, 8, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

func (n ID) HashID() string {
	s, err := hashid.EncodeInt64([]int64{int64(n)})
	if err != nil {
		panic(err)
	}
	return s
}

type Generator struct {
	mid    int64
	seq    int64
	lastTS int64
	m      sync.Mutex
}

func NewGenerator(machineID int) (*Generator, error) {
	if machineID&machineMask != machineID {
		return nil, fmt.Errorf("machine id must be in 10bit")
	}
	return &Generator{mid: int64(machineID)}, nil
}

func (g *Generator) Next() (ID, error) {
	g.m.Lock()
	defer g.m.Unlock()

	now := clock()
	ts := int64(float64(now-epoch) * 1e-6)
	if ts < g.lastTS {
		return 0, ErrClock
	}
	if ts == g.lastTS {
		g.seq = (g.seq + 1) & seqMask
		if g.seq == 0 {
			// sequencer is overflow, wait until next millisecond
			// TODO: return future object?
			time.Sleep(time.Millisecond - time.Duration(now%int64(time.Millisecond)))
		}
	} else {
		g.seq = 0
	}

	g.lastTS = ts
	return ID(ts<<22 | g.mid<<12 | g.seq), nil
}

func init() {
	SetEpoch(time.Date(2015, time.July, 1, 0, 0, 0, 0, time.UTC))
	SetSalt("pluq salt")
}
