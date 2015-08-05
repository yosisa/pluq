package bolt

import (
	"encoding/binary"
	"errors"
	"log"
	"time"

	"github.com/boltdb/bolt"
	"github.com/yosisa/pluq/event"
	"github.com/yosisa/pluq/storage"
	"github.com/yosisa/pluq/uid"
)

var (
	bucketMessage    = []byte("message")
	bucketSchedule   = []byte("schedule")
	bucketReplyIndex = []byte("replyIndex")
)

var (
	ErrBucketNotFound  = errors.New("Error bucket not found")
	ErrMessageNotFound = errors.New("Error message not found")

	errConflict = errors.New("Error conflict")
)

// scheduleKey represents key of schedule bucket.
type scheduleKey []byte

func newScheduleKey(queue string) scheduleKey {
	n := 8 + len(queue)
	b := make([]byte, n, n)
	copy(b[8:], []byte(queue))
	return scheduleKey(b)
}

func (b scheduleKey) timestamp() int64 {
	return int64(binary.BigEndian.Uint64(b[:8]))
}

func (b scheduleKey) setTimestamp(t int64) {
	binary.BigEndian.PutUint64(b, uint64(t))
}

func (b scheduleKey) queue() string {
	return string(b[8:])
}

type scheduleData []byte

func newScheduleData(id uid.ID, retry int32, timeout int64) scheduleData {
	n := 8 + 4*8
	b := make([]byte, n, n)
	copy(b, id.Bytes())
	binary.BigEndian.PutUint32(b[8:], uint32(retry))
	binary.BigEndian.PutUint64(b[12:], uint64(timeout))
	return b
}

func (b scheduleData) messageID() []byte {
	return b[:8]
}

func (b scheduleData) retry() int {
	return int(binary.BigEndian.Uint32(b[8:]))
}

func (b scheduleData) setRetry(n int) {
	binary.BigEndian.PutUint32(b[8:], uint32(n))
}

func (b scheduleData) timeout() int64 {
	return int64(binary.BigEndian.Uint64(b[12:]))
}

type replyData []byte

func newReplyData(msgid []byte, skey scheduleKey) replyData {
	b := make([]byte, 8+len(skey), 8+len(skey))
	copy(b, msgid)
	copy(b[8:], skey)
	return b
}

func (b replyData) expireAt() int64 {
	return int64(binary.BigEndian.Uint64(b[8:16]))
}

func (b replyData) messageID() []byte {
	return b[:8]
}

func (b replyData) scheduleID() []byte {
	return b[8:]
}

type Driver struct {
	db     *bolt.DB
	closed chan struct{}
}

func New(dbpath string) (*Driver, error) {
	db, err := bolt.Open(dbpath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	d := &Driver{
		db:     db,
		closed: make(chan struct{}),
	}
	go d.gc(time.Minute)
	return d, nil
}

func (d *Driver) Enqueue(queue string, id uid.ID, e *storage.Envelope) error {
	skey := newScheduleKey(queue)
	sval := newScheduleData(id, int32(e.Retry), int64(e.Timeout))
	b, err := marshal(e)
	if err != nil {
		return err
	}
	for {
		t := time.Now().UnixNano()
		skey.setTimestamp(t)
		err := d.db.Update(func(tx *bolt.Tx) error {
			message, err := tx.CreateBucketIfNotExists(bucketMessage)
			if err != nil {
				return err
			}
			schedule, err := tx.CreateBucketIfNotExists(bucketSchedule)
			if err != nil {
				return err
			}
			val := schedule.Get(skey)
			if val != nil {
				return errConflict
			}
			if err = message.Put(id.Bytes(), b); err != nil {
				return err
			}
			return schedule.Put(skey, sval)
		})
		if err == errConflict {
			continue
		}
		return err
	}
}

func (d *Driver) Dequeue(queue string, eid uid.ID) (e *storage.Envelope, err error) {
	var sd scheduleData
	now := time.Now().UnixNano()
	err = d.db.Update(func(tx *bolt.Tx) error {
		ridx, err := tx.CreateBucketIfNotExists(bucketReplyIndex)
		if err != nil {
			return err
		}
		schedule := tx.Bucket(bucketSchedule)
		if schedule == nil {
			return ErrBucketNotFound
		}
		message := tx.Bucket(bucketMessage)
		if schedule == nil {
			return ErrBucketNotFound
		}

		c := schedule.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			skey := scheduleKey(k)
			sval := scheduleData(v)
			if skey.timestamp() > now {
				return storage.ErrEmpty
			}
			if !storage.CanRetry(sval.retry()) {
				var envelope *storage.Envelope
				if b := message.Get(sval.messageID()); b != nil {
					envelope, _ = reconstruct(sval, b)
				}
				if err := schedule.Delete(k); err != nil {
					return err
				}
				if envelope != nil {
					event.Emit(event.EventMessageDiscarded, envelope)
				}
				continue
			}
			if skey.queue() == queue {
				newkey := scheduleKey(cloneBytes(k))
				sd = scheduleData(cloneBytes(v))
				if err := schedule.Delete(k); err != nil {
					return err
				}

				nextTick := now + sd.timeout()
				newkey.setTimestamp(nextTick)
				sd.setRetry(storage.DecrRetry(sd.retry()))
				if err := schedule.Put(newkey, sd); err != nil {
					return err
				}
				return ridx.Put(eid.Bytes(), newReplyData(sd.messageID(), scheduleKey(newkey)))
			}
		}
		return storage.ErrEmpty
	})
	if err != nil {
		return
	}

	var data []byte
	err = d.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketMessage)
		if bucket == nil {
			return ErrBucketNotFound
		}
		b := bucket.Get(sd.messageID())
		if b == nil {
			return ErrMessageNotFound
		}
		data = cloneBytes(b)
		return nil
	})
	if err != nil {
		return
	}
	return reconstruct(sd, data)
}

func (d *Driver) Ack(eid uid.ID) (err error) {
	var rd replyData
	err = d.db.View(func(tx *bolt.Tx) error {
		ridx := tx.Bucket(bucketReplyIndex)
		if ridx == nil {
			return ErrBucketNotFound
		}
		v := ridx.Get(eid.Bytes())
		if v == nil {
			return storage.ErrInvalidEphemeralID
		}
		rd = replyData(cloneBytes(v))
		return nil
	})
	if err != nil {
		return
	}
	if rd.expireAt() <= time.Now().UnixNano() { // expired
		return storage.ErrInvalidEphemeralID
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		schedule := tx.Bucket(bucketSchedule)
		if schedule == nil {
			return ErrBucketNotFound
		}
		message := tx.Bucket(bucketMessage)
		if message == nil {
			return ErrBucketNotFound
		}
		if schedule.Get(rd.scheduleID()) == nil {
			return storage.ErrInvalidEphemeralID
		}
		if err := schedule.Delete(rd.scheduleID()); err != nil {
			return err
		}
		return message.Delete(rd.messageID())
	})
}

func (d *Driver) Close() error {
	close(d.closed)
	return d.db.Close()
}

func (d *Driver) gc(interval time.Duration) {
	tick := time.Tick(interval)
	for {
		select {
		case <-tick:
		case <-d.closed:
			return
		}
		now := time.Now().UnixNano()
		err := d.db.Update(func(tx *bolt.Tx) error {
			ridx := tx.Bucket(bucketReplyIndex)
			if ridx == nil {
				return nil
			}
			c := ridx.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if replyData(v).expireAt() <= now {
					if err := ridx.Delete(k); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			log.Print(err)
		}
	}
}

func cloneBytes(s []byte) []byte {
	n := len(s)
	b := make([]byte, n, n)
	copy(b, s)
	return b
}
