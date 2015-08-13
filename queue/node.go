package queue

import (
	"sync"
	"time"
)

type Properties struct {
	Retry     *int
	Timeout   *time.Duration
	AccumTime *time.Duration
}

func NewProperties() *Properties {
	return &Properties{}
}

func (p *Properties) SetRetry(n int) *Properties {
	p.Retry = &n
	return p
}

func (p *Properties) SetTimeout(d time.Duration) *Properties {
	p.Timeout = &d
	return p
}

func (p *Properties) SetAccumTime(d time.Duration) *Properties {
	p.AccumTime = &d
	return p
}

func (p *Properties) merge(other *Properties) {
	if other.Retry != nil {
		p.SetRetry(*other.Retry)
	}
	if other.Timeout != nil {
		p.SetTimeout(*other.Timeout)
	}
	if other.AccumTime != nil {
		p.SetAccumTime(*other.AccumTime)
	}
}

type node struct {
	children map[string]*node
	props    *Properties
	m        sync.Mutex
}

func newNode() *node {
	return &node{children: make(map[string]*node)}
}

func (n *node) properties(keys []string) *Properties {
	props := NewProperties()
	n.mergeProperties(keys, props)
	return props
}

func (n *node) mergeProperties(keys []string, props *Properties) {
	if n.props != nil {
		props.merge(n.props)
	}
	if len(keys) == 0 {
		return
	}
	if child := n.children[keys[0]]; child != nil {
		child.mergeProperties(keys[1:], props)
	}
}

func (n *node) setProperties(keys []string, props *Properties) {
	if len(keys) == 0 {
		n.props = props
		return
	}
	n.m.Lock()
	defer n.m.Unlock()
	child := n.children[keys[0]]
	if child == nil {
		child = newNode()
		n.children[keys[0]] = child
	}
	child.setProperties(keys[1:], props)
}

func (n *node) lookup(keys []string) *node {
	if len(keys) == 0 {
		return n
	}
	if child := n.children[keys[0]]; child != nil {
		return child.lookup(keys[1:])
	}
	return nil
}
