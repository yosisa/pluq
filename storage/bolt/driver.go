package bolt

import (
	"encoding/binary"
	"errors"
	"log"
	"time"

	"github.com/boltdb/bolt"
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

func (d *Driver) Enqueue(queue string, id uid.ID, body []byte) error {
	skey := newScheduleKey(queue)
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
			if err = message.Put(id.Bytes(), body); err != nil {
				return err
			}
			val := schedule.Get(skey)
			if val != nil {
				return errConflict
			}
			return schedule.Put(skey, id.Bytes())
		})
		if err == errConflict {
			continue
		}
		return err
	}
}

func (d *Driver) Dequeue(queue string, eid uid.ID) (body []byte, err error) {
	var msgid []byte
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
		c := schedule.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			skey := scheduleKey(k)
			if skey.timestamp() > now {
				return storage.ErrEmpty
			}
			if skey.queue() == queue {
				newkey := cloneBytes(k)
				msgid = cloneBytes(v)
				if err := schedule.Delete(k); err != nil {
					return err
				}
				nextTick := now + int64(storage.RetryWait)
				scheduleKey(newkey).setTimestamp(nextTick)
				if err := schedule.Put(newkey, msgid); err != nil {
					return err
				}
				return ridx.Put(eid.Bytes(), newReplyData(msgid, scheduleKey(newkey)))
			}
		}
		return storage.ErrEmpty
	})
	if err != nil {
		return
	}
	err = d.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketMessage)
		if bucket == nil {
			return ErrBucketNotFound
		}
		b := bucket.Get(msgid)
		if b == nil {
			return ErrMessageNotFound
		}
		body = cloneBytes(b)
		return nil
	})
	return
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
