package walker

import (
	"context"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

type Ordering int64

const (
	DepthFirst Ordering = iota
	BreadthFirst
)

type Config struct {
	Ctx                            context.Context
	LinkSystem                     ipld.LinkSystem                          // LinkSystem used for automatic link loading, and also any storing if mutation features (e.g. traversal.Transform) are used.
	LinkTargetNodePrototypeChooser traversal.LinkTargetNodePrototypeChooser // Chooser for Node implementations to produce during automatic link traversal.
	Ordering                       Ordering
	Root                           ipld.Node
	Selector                       selector.Selector
	Visitor                        AdvVisitFn
}

type Progress struct {
	Path      ipld.Path // Path is how we reached the current point in the traversal.
	LastBlock struct {  // LastBlock stores the Path and Link of the last block edge we had to load.  (It will always be zero in traversals with no linkloader.)
		Path ipld.Path
		Link ipld.Link
	}
}

// AdvVisitFn is like VisitFn, but for use with AdvTraversal: it gets additional arguments describing *why* this node is visited.
type AdvVisitFn func(Progress, ipld.Node, traversal.VisitReason) error

// VisitFn is a read-only visitor.
type VisitFn func(Progress, ipld.Node) error

// TransformFn is like a visitor that can also return a new Node to replace the visited one.
type TransformFn func(Progress, ipld.Node) (ipld.Node, error)
