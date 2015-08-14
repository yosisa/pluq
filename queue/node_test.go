package queue

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/yosisa/pluq/types"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type NodeSuite struct{}

var _ = Suite(&NodeSuite{})

func (s *NodeSuite) TestProperties(c *C) {
	root := newNode()
	rootProps := NewProperties().SetRetry(1)
	root.setProperties([]string{}, rootProps)
	props := root.properties([]string{})
	c.Assert(props, DeepEquals, rootProps)

	root.setProperties([]string{"a", "b"}, NewProperties().SetTimeout(types.Duration(time.Second)))
	props = root.properties([]string{})
	c.Assert(props, DeepEquals, rootProps)
	props = root.properties([]string{"a"})
	c.Assert(props, DeepEquals, rootProps)
	props = root.properties([]string{"a", "b"})
	c.Assert(props, DeepEquals, NewProperties().SetRetry(1).SetTimeout(types.Duration(time.Second)))

	root.setProperties([]string{"a"}, NewProperties().SetRetry(2))
	props = root.properties([]string{"a", "b"})
	c.Assert(props, DeepEquals, NewProperties().SetRetry(2).SetTimeout(types.Duration(time.Second)))
	props = root.properties([]string{"a", "b", "c"})
	c.Assert(props, DeepEquals, NewProperties().SetRetry(2).SetTimeout(types.Duration(time.Second)))
	props = root.properties([]string{"a", "c"})
	c.Assert(props, DeepEquals, NewProperties().SetRetry(2))
}

func (s *NodeSuite) TestFindQueue(c *C) {
	root := newNode()
	q := root.findQueue([]string{"a"})
	c.Assert(q, HasLen, 1)
	c.Assert(q[0].keys, DeepEquals, []string{"a"})
	c.Assert(q[0].props, DeepEquals, NewProperties())

	root.setProperties([]string{"a"}, NewProperties().SetRecurse(true))
	q = root.findQueue([]string{"a"})
	c.Assert(q, HasLen, 1)
	c.Assert(q[0].keys, DeepEquals, []string{"a"})
	c.Assert(q[0].props, DeepEquals, NewProperties().SetRecurse(true))

	q = root.findQueue([]string{"a", "b"})
	c.Assert(q, HasLen, 1)
	c.Assert(q[0].keys, DeepEquals, []string{"a", "b"})
	c.Assert(q[0].props, DeepEquals, NewProperties().SetRecurse(true))

	root.setProperties([]string{"a", "b"}, NewProperties().SetRetry(1))
	root.setProperties([]string{"a", "c"}, NewProperties())
	q = root.findQueue([]string{"a"})
	sort.Sort(ByName(q))
	c.Assert(q, HasLen, 3)
	c.Assert(q[0].keys, DeepEquals, []string{"a"})
	c.Assert(q[0].props, DeepEquals, NewProperties().SetRecurse(true))
	c.Assert(q[1].keys, DeepEquals, []string{"a", "b"})
	c.Assert(q[1].props, DeepEquals, NewProperties().SetRecurse(true).SetRetry(1))
	c.Assert(q[2].keys, DeepEquals, []string{"a", "c"})
	c.Assert(q[2].props, DeepEquals, NewProperties().SetRecurse(true))

	// Check inheritance
	root.setProperties([]string{"a", "b", "c"}, NewProperties())
	q = root.findQueue([]string{"a", "b"})
	c.Assert(q, HasLen, 2)
	c.Assert(q[0].keys, DeepEquals, []string{"a", "b"})
	c.Assert(q[0].props, DeepEquals, NewProperties().SetRecurse(true).SetRetry(1))
	c.Assert(q[1].keys, DeepEquals, []string{"a", "b", "c"})
	c.Assert(q[1].props, DeepEquals, NewProperties().SetRecurse(true).SetRetry(1))

	root.setProperties([]string{"a", "b"}, NewProperties().SetRecurse(false))
	q = root.findQueue([]string{"a"})
	sort.Sort(ByName(q))
	c.Assert(q, HasLen, 3)
	c.Assert(q[0].keys, DeepEquals, []string{"a"})
	c.Assert(q[0].props, DeepEquals, NewProperties().SetRecurse(true))
	c.Assert(q[1].keys, DeepEquals, []string{"a", "b"})
	c.Assert(q[1].props, DeepEquals, NewProperties().SetRecurse(false))
	c.Assert(q[2].keys, DeepEquals, []string{"a", "c"})
	c.Assert(q[2].props, DeepEquals, NewProperties().SetRecurse(true))
}

type ByName []*queue

func (p ByName) Len() int { return len(p) }

func (p ByName) Less(i, j int) bool {
	return strings.Join(p[i].keys, "/") < strings.Join(p[i].keys, "/")
}

func (p ByName) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
