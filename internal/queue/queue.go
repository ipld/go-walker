package queue

import (
	"errors"
	"fmt"

	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	walker "github.com/ipld/go-walker/pkg"
)

type QueuedNode struct {
	Node     ipld.Node
	Selector selector.Selector
	walker.Progress
}

type OrderedQueue struct {
	ordering    walker.Ordering
	queuedNodes []QueuedNode
}

func (s *OrderedQueue) Empty() bool {
	return len(s.queuedNodes) == 0
}

type errorType string

func (e errorType) Error() string {
	return string(e)
}

const ErrEmpty = errorType("Queue is empty")

const defaultQueueSize = 128

func NewOrderedQueue(ordering walker.Ordering) *OrderedQueue {
	return &OrderedQueue{
		ordering:    ordering,
		queuedNodes: make([]QueuedNode, 0, defaultQueueSize),
	}
}

func (s *OrderedQueue) Dequeue() (QueuedNode, error) {
	if s.Empty() {
		return QueuedNode{}, ErrEmpty
	}
	if s.ordering == walker.DepthFirst {
		queuedNode := s.queuedNodes[len(s.queuedNodes)-1]
		s.queuedNodes = s.queuedNodes[:len(s.queuedNodes)-1]
		return queuedNode, nil
	}
	queuedNode := s.queuedNodes[0]
	s.queuedNodes = s.queuedNodes[1:]
	return queuedNode, nil
}

func (s *OrderedQueue) Enqueue(newNodes []QueuedNode) {
	if s.ordering == walker.DepthFirst {
		// reverse input order
		for i := len(newNodes)/2 - 1; i >= 0; i-- {
			opp := len(newNodes) - 1 - i
			newNodes[i], newNodes[opp] = newNodes[opp], newNodes[i]
		}
	}
	s.queuedNodes = append(s.queuedNodes, newNodes...)
}

func (q QueuedNode) IsResolved() bool {
	return q.Node.Kind() != ipld.Kind_Link
}

func (q QueuedNode) Resolve(config walker.Config) (QueuedNode, error) {
	lnk, lnkCtx, err := q.ResolveParameters(config)
	if err != nil {
		return QueuedNode{}, err
	}
	// Pick what in-memory format we will build.
	np, err := config.LinkTargetNodePrototypeChooser(lnk, lnkCtx)
	if err != nil {
		return QueuedNode{}, fmt.Errorf("error traversing node at %q: could not load link %q: %s", q.Path, lnk, err)
	}
	// Load link!
	n, err := config.LinkSystem.Load(lnkCtx, lnk, np)
	if err != nil {
		if _, ok := err.(traversal.SkipMe); ok {
			return QueuedNode{}, err
		}
		return QueuedNode{}, fmt.Errorf("error traversing node at %q: could not load link %q: %s", q.Path, lnk, err)
	}
	return QueuedNode{
		Node:     n,
		Selector: q.Selector,
		Progress: walker.Progress{
			Path: q.Path,
			LastBlock: struct {
				Path ipld.Path
				Link ipld.Link
			}{q.Path, lnk},
		},
	}, nil
}

func (q QueuedNode) ResolveParameters(config walker.Config) (ipld.Link, ipld.LinkContext, error) {
	lnk, err := q.Node.AsLink()
	if err != nil {
		return nil, ipld.LinkContext{}, err
	}
	return lnk, ipld.LinkContext{
		Ctx:      config.Ctx,
		LinkPath: q.Path,
		LinkNode: q.Node,
	}, nil
}

func (q QueuedNode) Visit(fn walker.AdvVisitFn) error {
	if !q.IsResolved() {
		return errors.New("Cannot visit unresolved nodes")
	}
	n := q.Node
	s := q.Selector
	if s.Decide(n) {
		if err := fn(q.Progress, n, traversal.VisitReason_SelectionMatch); err != nil {
			return err
		}
	} else {
		if err := fn(q.Progress, n, traversal.VisitReason_SelectionCandidate); err != nil {
			return err
		}
	}
	return nil
}

func (q QueuedNode) Children() ([]QueuedNode, error) {
	if !q.IsResolved() {
		return nil, errors.New("Cannot get children of unresolved node")
	}
	n := q.Node
	s := q.Selector
	nk := n.Kind()
	switch nk {
	case ipld.Kind_Map, ipld.Kind_List: // continue
	default:
		return nil, nil
	}
	attn := s.Interests()
	if attn == nil {
		return q.walkAdv_iterateAll()
	}
	return q.walkAdv_iterateSelective(attn)
}

func (q QueuedNode) walkAdv_iterateAll() ([]QueuedNode, error) {
	n := q.Node
	s := q.Selector
	newNodes := make([]QueuedNode, 0, int(n.Length()))
	for itr := selector.NewSegmentIterator(n); !itr.Done(); {
		ps, v, err := itr.Next()
		if err != nil {
			return nil, err
		}
		sNext := s.Explore(n, ps)
		if sNext != nil {
			progNext := q.Progress
			progNext.Path = q.Progress.Path.AppendSegment(ps)
			newNodes = append(newNodes, QueuedNode{v, sNext, progNext})
		}
	}
	return newNodes, nil
}

func (q QueuedNode) walkAdv_iterateSelective(attn []ipld.PathSegment) ([]QueuedNode, error) {
	n := q.Node
	s := q.Selector
	newNodes := make([]QueuedNode, 0, len(attn))
	for _, ps := range attn {
		v, err := n.LookupBySegment(ps)
		if err != nil {
			continue
		}
		sNext := s.Explore(n, ps)
		if sNext != nil {
			progNext := q.Progress
			progNext.Path = q.Progress.Path.AppendSegment(ps)
			newNodes = append(newNodes, QueuedNode{v, sNext, progNext})
		}
	}
	return newNodes, nil
}
