package queue

import (
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
