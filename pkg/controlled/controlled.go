package controlled

import (
	"errors"
	"io"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-walker/internal/queue"
	walker "github.com/ipld/go-walker/pkg"
)

// NewControlledWalk performing a selector traversal that operates iteratively --
// it stops and waits for a manual load every time a block boundary is encountered
func NewControlledWalk(config walker.Config) (*ControlledWalker, error) {
	if config.Parallelism > 1 {
		return nil, errors.New("Controlled walk does not support parellelism")
	}
	cw := &ControlledWalker{
		config: config,
		queue:  queue.NewOrderedQueue(config.Ordering),
		nextNode: queue.QueuedNode{
			Node:     config.Root,
			Selector: config.Selector,
		},
	}
	cw.config.LinkSystem.StorageReadOpener = cw.storageReadOpener
	return cw, nil
}

type nextResponse struct {
	input io.Reader
	err   error
}

// ControlledWalker is a class to perform a selector traversal that stops every time a new block is loaded
// and waits for manual input (in the form of advance or error)
type ControlledWalker struct {
	config        walker.Config
	queue         *queue.OrderedQueue
	nextResponse  nextResponse
	nextNode      queue.QueuedNode
	isDone        bool
	completionErr error
}

func (t *ControlledWalker) storageReadOpener(lnkCtx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
	return t.nextResponse.input, t.nextResponse.err
}

func (t *ControlledWalker) resolve() error {
	nextNode, err := t.nextNode.Resolve(t.config)
	if err != nil {
		t.isDone = true
		t.completionErr = err
		return err
	}
	t.nextNode = nextNode
	return nil
}

// IsComplete returns true if a traversal is complete
func (t *ControlledWalker) IsComplete() (bool, error) {
	t.isDone, t.completionErr = t.nextLink()
	return t.isDone, t.completionErr
}

// CurrentRequest returns the current block load waiting to be fulfilled in order
// to advance further
func (t *ControlledWalker) CurrentRequest() (ipld.Link, ipld.LinkContext, error) {
	isComplete, _ := t.IsComplete()
	if isComplete {
		return nil, ipld.LinkContext{}, errors.New("traversal done no current request")
	}
	return t.nextNode.ResolveParameters(t.config)
}

// Advance advances the traversal with an io.Reader for the next requested block
func (t *ControlledWalker) Advance(reader io.Reader) error {
	isComplete, _ := t.IsComplete()
	if isComplete {
		return errors.New("cannot advance when done")
	}
	t.nextResponse = nextResponse{reader, nil}
	return t.resolve()
}

// Error aborts the traversal with an error for the next block load
func (t *ControlledWalker) Error(err error) {
	isComplete, _ := t.IsComplete()
	if isComplete {
		return
	}
	t.nextResponse = nextResponse{nil, err}
	_ = t.resolve()
}

func (t *ControlledWalker) nextLink() (bool, error) {
	for t.nextNode.IsResolved() {
		if t.config.VisitRoot || t.nextNode.Node != t.config.Root {
			err := t.nextNode.Visit(t.config.Visitor)
			if err != nil {
				return true, err
			}
		}
		newNodes, err := t.nextNode.Children()
		if err != nil {
			return true, err
		}
		t.queue.Enqueue(newNodes)
		if t.queue.Empty() {
			return true, nil
		}
		t.nextNode, _ = t.queue.Dequeue()
	}
	return false, nil
}
