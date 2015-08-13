package queue

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type NodeSuite struct{}

var _ = Suite(&NodeSuite{})

func (s *NodeSuite) TestProperties(c *C) {
	root := newNode()
	rootProps := NewProperties().Retry(1)
	root.setProperties([]string{}, rootProps)
	props := root.properties([]string{})
	c.Assert(props, DeepEquals, rootProps)

	root.setProperties([]string{"a", "b"}, NewProperties().Timeout(time.Second))
	props = root.properties([]string{})
	c.Assert(props, DeepEquals, rootProps)
	props = root.properties([]string{"a"})
	c.Assert(props, DeepEquals, rootProps)
	props = root.properties([]string{"a", "b"})
	c.Assert(props, DeepEquals, NewProperties().Retry(1).Timeout(time.Second))

	root.setProperties([]string{"a"}, NewProperties().Retry(2))
	props = root.properties([]string{"a", "b"})
	c.Assert(props, DeepEquals, NewProperties().Retry(2).Timeout(time.Second))
	props = root.properties([]string{"a", "b", "c"})
	c.Assert(props, DeepEquals, NewProperties().Retry(2).Timeout(time.Second))
	props = root.properties([]string{"a", "c"})
	c.Assert(props, DeepEquals, NewProperties().Retry(2))
}
