package queue

import (
	"sync"

	"github.com/yosisa/pluq/types"
)

type Properties struct {
	Retry     *types.Retry    `json:"retry,omitempty"`
	Timeout   *types.Duration `json:"timeout,omitempty"`
	AccumTime *types.Duration `json:"accum_time,omitempty"`
	Recurse   *bool           `json:"recurse,omitempty"`
}

func NewProperties() *Properties {
	return &Properties{}
}

func (p *Properties) SetRetry(n types.Retry) *Properties {
	p.Retry = &n
	return p
}

func (p *Properties) SetTimeout(d types.Duration) *Properties {
	p.Timeout = &d
	return p
}

func (p *Properties) SetAccumTime(d types.Duration) *Properties {
	p.AccumTime = &d
	return p
}

func (p *Properties) SetRecurse(b bool) *Properties {
	p.Recurse = &b
	return p
}

func (p *Properties) merge(other *Properties) {
	if other == nil {
		return
	}
	if other.Retry != nil {
		p.SetRetry(*other.Retry)
	}
	if other.Timeout != nil {
		p.SetTimeout(*other.Timeout)
	}
	if other.AccumTime != nil {
		p.SetAccumTime(*other.AccumTime)
	}
	if other.Recurse != nil {
		p.SetRecurse(*other.Recurse)
	}
}

type node struct {
	children *nodeMap
	props    *Properties
}

func newNode() *node {
	return &node{children: newNodeMap()}
}

func (n *node) properties(keys []string) *Properties {
	props := NewProperties()
	n.mergeProperties(keys, props)
	return props
}

func (n *node) mergeProperties(keys []string, props *Properties) *node {
	if n.props != nil {
		props.merge(n.props)
	}
	if len(keys) == 0 {
		return n
	}
	return n.children.get(keys[0]).mergeProperties(keys[1:], props)
}

func (n *node) setProperties(keys []string, props *Properties) {
	if len(keys) == 0 {
		n.props = props
		return
	}
	n.children.get(keys[0]).setProperties(keys[1:], props)
}

func (n *node) lookup(keys []string) *node {
	if len(keys) == 0 {
		return n
	}
	return n.children.get(keys[0]).lookup(keys[1:])
}

func (n *node) findQueue(keys []string) []*queue {
	props := NewProperties()
	node := n.mergeProperties(keys, props)
	return node.findQueueRecurse(keys, *props)
}

func (n *node) findQueueRecurse(keys []string, props Properties) []*queue {
	props.merge(n.props)
	targets := []*queue{{keys: keys, props: &props}}
	if props.Recurse == nil || !*props.Recurse {
		return targets
	}
	n.children.RLock()
	defer n.children.RUnlock()
	for name, child := range n.children.m {
		childKeys := make([]string, len(keys)+1)
		copy(childKeys, keys)
		childKeys[len(keys)] = name
		targets = append(targets, child.findQueueRecurse(childKeys, props)...)
	}
	return targets
}

type nodeMap struct {
	sync.RWMutex
	m map[string]*node
}

func newNodeMap() *nodeMap {
	return &nodeMap{m: make(map[string]*node)}
}

func (m *nodeMap) get(name string) *node {
	m.RLock()
	n := m.m[name]
	m.RUnlock()
	if n == nil {
		m.Lock()
		if n = m.m[name]; n == nil {
			n = newNode()
			m.m[name] = n
		}
		m.Unlock()
	}
	return n
}

type queue struct {
	keys  []string
	props *Properties
}
