package types

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	RootElementId uint64 = 1
)

type FlameGraphNode struct {
	Id          uint64            `json:"-"`
	Cluster     string            `json:"-"`
	Name        string            `json:"name"`
	Total       uint64            `json:"total"`
	Value       uint64            `json:"value"`
	ModTime     int64             `json:"mtime,omitempty"`
	RdTime      int64             `json:"rdtime,omitempty"`
	ATime       int64             `json:"atime,omitempty"`
	Count       uint64            `json:"count,omitempty"`
	Children    []*FlameGraphNode `json:"children,omitempty"`
	ChildrenIds []uint64          `json:"-"`
	Parent      *FlameGraphNode   `json:"-"`
}

type sampleToNodeMap struct {
	sync.RWMutex
	currentId      int64
	samplesToNodes map[string]*StackFlameGraphNode
}

type StackFlameGraphNode struct {
	Id           int64                  `json:"id"`
	Application  string                 `json:"application"`
	Instance     string                 `json:"instance"`
	FunctionName string                 `json:"name"`
	FileName     string                 `json:"file"`
	Line         int64                  `json:"line"`
	Samples      int64                  `json:"samples"`
	MaxSamples   int64                  `json:"maxSamples"`
	Children     []*StackFlameGraphNode `json:"children,omitempty"`
	ChildrenIds  []int64                `json:"childrenIds"`
	Parent       *StackFlameGraphNode   `json:"-"`
	ParentID     int64                  `json:"parentId"`
	root         *StackFlameGraphNode   `json:"root"`

	metadata *sampleToNodeMap
}

func NewStackFlamegraphTree(name, instance, app string) *StackFlameGraphNode {
	fg := &StackFlameGraphNode{
		Id:           int64(RootElementId),
		Application:  app,
		FunctionName: name,
		Samples:      0,
		Parent:       nil,
		ParentID:     0,
		Instance:     instance,
		root:         nil,

		metadata: &sampleToNodeMap{
			currentId:      int64(RootElementId),
			samplesToNodes: make(map[string]*StackFlameGraphNode),
		},
	}

	fg.root = fg
	return fg
}

func (r *StackFlameGraphNode) Increment(stackSamples int64) {
	atomic.AddInt64(&r.Samples, stackSamples)
}

func (r *StackFlameGraphNode) FindOrAdd(funcName, fileName string, fileLine int64, stackSamples int64) *StackFlameGraphNode {
	r.Samples += stackSamples
	k := fmt.Sprintf("%v:%v:%v", funcName, fileName, fileLine)

	r.metadata.Lock()
	defer r.metadata.Unlock()
	if n, ok := r.metadata.samplesToNodes[k]; ok {
		return n
	}

	r.metadata.currentId++
	s := &StackFlameGraphNode{
		Id:           r.metadata.currentId,
		Application:  r.root.Application,
		FunctionName: funcName,
		FileName:     fileName,
		Line:         fileLine,
		Samples:      stackSamples,
		MaxSamples:   r.root.MaxSamples,
		Parent:       r,
		ParentID:     r.Id,
		Instance:     r.root.Instance,
		root:         r.root,
		metadata:     r.root.metadata,
	}
	r.ChildrenIds = append(r.ChildrenIds, s.Id)
	r.Children = append(r.Children, s)
	r.metadata.samplesToNodes[k] = s

	return s
}

type Metrics struct {
	Metrics []string `json:"Metrics"`
}

type Cluster struct {
	Name  string
	Hosts []string
}

type ClickhouseField struct {
	Timestamp   int64
	GraphType   string
	Cluster     string
	Name        string
	Total       uint64
	Id          uint64
	Value       uint64
	ModTime     int64
	Level       uint64
	ParentID    uint64
	ChildrenIds []uint64
}
