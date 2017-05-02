package internal

type FlameGraphNode struct {
	Id          uint64 `json:"-"`
	Cluster     string `json:"-"`
	Name        string            `json:"name"`
	Total       uint64            `json:"total"`
	Value       uint64            `json:"value"`
	ModTime     int64             `json:"mtime,omitempty"`
	RdTime      int64             `json:"rdtime,omitempty"`
	ATime       int64             `json:"atime,omitempty"`
	Count       uint64           `json:"count,omitempty"`
	Children    []*FlameGraphNode `json:"children,omitempty"`
	ChildrenIds []uint64 `json:"-"`
	Parent      *FlameGraphNode `json:"-"`
}

type Metrics struct {
	Metrics []string `json:"Metrics"`
}

type Cluster struct {
	Name  string
	Hosts []string
}

const (
	RootElementId uint64 = 1
)

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