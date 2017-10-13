package types

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

const (
	RootElementId uint64 = 1

	FieldSeparator string = "$"
)

type FlameGraphNode struct {
	Id          uint64            `json:"-"`
	Cluster     string            `json:"-"`
	Name        string            `json:"name"`
	Total       uint64             `json:"total"`
	Value       uint64             `json:"value"`
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
	samplesToNodes map[string]*StackFlameGraphNode
}

type StackFlameGraphNode struct {
	Id           uint64                 `json:"id"`
	Application  string                 `json:"application"`
	Instance     string                 `json:"instance"`
	FunctionName string                 `json:"name"`
	FileName     string                 `json:"file"`
	Line         int64                  `json:"line"`
	Samples      int64                  `json:"samples"`
	MaxSamples   int64                  `json:"maxSamples"`
	Children     []*StackFlameGraphNode `json:"children,omitempty"`
	ChildrenIds  []uint64               `json:"childrenIds"`
	Parent       *StackFlameGraphNode   `json:"-"`
	ParentID     uint64                 `json:"parentId"`
	IsRoot       uint8                  `json:"isRoot"`
	FullName     string                 `json:"fullName"`

	root *StackFlameGraphNode

	metadata *sampleToNodeMap
}

func nameToId(name string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(name))
	return uint64(hash.Sum64())
}

func NewStackFlamegraphTree(name, instance, app string) *StackFlameGraphNode {
	fg := &StackFlameGraphNode{
		Id:           nameToId(name),
		Application:  app,
		FunctionName: name,
		Samples:      0,
		Parent:       nil,
		ParentID:     0,
		Instance:     instance,
		FullName:     name,
		IsRoot:       1,
		root:         nil,

		metadata: &sampleToNodeMap{
			samplesToNodes: make(map[string]*StackFlameGraphNode),
		},
	}

	fg.root = fg
	return fg
}

func (r *StackFlameGraphNode) Increment(stackSamples int64) {
	atomic.AddInt64(&r.Samples, stackSamples)
}

func (r *StackFlameGraphNode) FindOrAdd(funcName, fileName string, fileLine int64, fullName string, stackSamples int64) *StackFlameGraphNode {
	r.Samples += stackSamples
	k := fmt.Sprintf("%v:%v:%v", funcName, fileName, fileLine)

	r.metadata.Lock()
	defer r.metadata.Unlock()
	if n, ok := r.metadata.samplesToNodes[k]; ok {
		return n
	}

	s := &StackFlameGraphNode{
		Id:           nameToId(fullName),
		Application:  r.root.Application,
		FunctionName: funcName,
		FileName:     fileName,
		Line:         fileLine,
		Samples:      stackSamples,
		MaxSamples:   r.root.MaxSamples,
		Parent:       r,
		FullName:     fullName,
		ParentID:     r.Id,
		Instance:     r.root.Instance,
		IsRoot:       0,
		root:         r.root,
		metadata:     r.root.metadata,
	}
	r.ChildrenIds = append(r.ChildrenIds, uint64(s.Id))
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
