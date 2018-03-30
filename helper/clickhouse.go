package helper

import (
	"fmt"
	"time"

	"database/sql"

	"github.com/kshvakov/clickhouse"

	fgpb "github.com/Civil/ch-flamegraphs/flamegraphpb"
	"github.com/Civil/ch-flamegraphs/types"
	"bytes"
	"net/http"
	"net"
	"sync/atomic"
	"net/url"
	"io/ioutil"
	"context"
	"go.uber.org/zap"
	"github.com/lomik/zapwriter"
	"strconv"
)

type ClickhouseConfig struct {
	ClickhouseServers []string `yaml:"servers"`
	ClickhouseHost string `yaml:"host"`
	Cluster        string `yaml:"cluster"`
	RowsPerInsert  int    `yaml:"rows_per_insert"`

	UseDistributedTables   bool   `yaml:"use_distributed_tables"`
	DistributedClusterName string `yaml:"distributed_cluster_name"`
}

func DBStartTransaction(db *sql.DB, query string) (*sql.Tx, *sql.Stmt, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, nil, err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return nil, nil, err
	}

	return tx, stmt, nil
}

type ClickhouseSender struct {
	db            *sql.DB
	tx            *sql.Tx
	stmt          *sql.Stmt
	linesToBuffer int
	lines         int
	commitedLines int64
	version       uint64
	now           time.Time
	txStart       time.Time

	query string

	isHTTP bool
	sendBuffer []byte
}

func NewClickhouseSender(db *sql.DB, query string, t int64, rowsPerInsert int) (*ClickhouseSender, error) {
	tx, stmt, err := DBStartTransaction(db, query)
	if err != nil {
		return nil, err
	}
	return &ClickhouseSender{
		db:            db,
		tx:            tx,
		stmt:          stmt,
		version:       uint64(t),
		now:           time.Now(),
		txStart:       time.Now(),
		linesToBuffer: rowsPerInsert,
		query:         query,
	}, nil
}

func (c *ClickhouseSender) startTransaction() error {
	var err error
	c.tx, c.stmt, err = DBStartTransaction(c.db, c.query)
	if err != nil {
		return err
	}
	c.commitedLines += int64(c.lines)
	c.lines = 0
	c.txStart = time.Now()
	return nil
}

// "INSERT INTO stacktraceTimestamps (Timestamp, Application, Instance, Date, Version)
func (c *ClickhouseSender) SendStacktraceTimestamp(data *types.StackFlameGraphNode) error {
	c.lines++
	_, err := c.stmt.Exec(
		int64(c.version),
		data.Application,
		data.Instance,
		c.now,
		c.version,
	)
	if err != nil {
		return err
	}

	if c.lines >= c.linesToBuffer || time.Since(c.txStart) > 280*time.Second {
		err = c.tx.Commit()
		if err != nil {
			return err
		}
		err = c.startTransaction()
		if err != nil {
			return err
		}
	}

	return err
}

func (c *ClickhouseSender) SendStacktrace(data *types.StackFlameGraphNode, fullName string) error {
	c.lines++

	parentID := int64(0)
	if data.Parent != nil {
		parentID = data.Parent.Id
	}
	_, err := c.stmt.Exec(
		int64(c.version),
		data.Id,
		data.Application,
		data.Instance,
		data.FunctionName,
		data.FileName,
		data.Line,
		data.Samples,
		data.MaxSamples,
		data.FullName,
		data.IsRoot,
		clickhouse.Array(data.ChildrenIds),
		parentID,
		c.now,
		c.version,
	)
	if err != nil {
		return err
	}

	if c.lines >= c.linesToBuffer || time.Since(c.txStart) > 280*time.Second {
		err = c.tx.Commit()
		if err != nil {
			return err
		}
		c.tx, c.stmt, err = DBStartTransaction(c.db, c.query)
		if err != nil {
			return err
		}
		c.commitedLines += int64(c.lines)
		c.lines = 0
		c.txStart = time.Now()
	}

	return err
}

type ClickhouseHTTPSender struct {
	query string
	lineCount int
	servers []string

	buffer *bytes.Buffer

	client *http.Client
	counter uint64
	logger *zap.Logger
	now time.Time
	nowStr string
}

func NewClickhouseHTTPSender(servers []string, query string) *ClickhouseHTTPSender {
	if len(servers) == 0 {
		return nil
	}

	return &ClickhouseHTTPSender{
		query: query,
		buffer: nil,
		lineCount: 0,
		servers: servers,
		logger: zapwriter.Logger("clickhouse http sender"),

		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 100,
				DialContext: (&net.Dialer{
					Timeout:   1 * time.Second,
					KeepAlive: 30 * time.Second,
					DualStack: true,
				}).DialContext,
			},
		},
	}
}

func (c *ClickhouseHTTPSender) pickServer() string {
	if len(c.servers) == 1 {
		return c.servers[0]
	}

	counter := atomic.AddUint64(&(c.counter), 1)
	idx := counter % uint64(len(c.servers))
	return c.servers[int(idx)]
}

func (c *ClickhouseHTTPSender) NewRequest() error {
	b := make([]byte, 0, 1024*1024)
	c.buffer = bytes.NewBuffer(b)
	c.now = time.Now()
	c.nowStr = c.now.Format("2006-01-02")
	return nil
}


func (c *ClickhouseHTTPSender) Commit() error {
	if c.lineCount == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120 * time.Second)
	defer cancel()

	server := c.pickServer()

	rewrite, _ := url.Parse("http://127.0.0.1/")

	v := url.Values{
		"query": []string{c.query + " FORMAT TabSeparated"},
	}
	rewrite.RawQuery = v.Encode()

	u, err := url.Parse("http://" + server + ":8123" + rewrite.RequestURI())
	if err != nil {
		return err
	}

	c.logger.Debug("doing request",
		zap.String("uri", u.String()),
	)

	req, err := http.NewRequest("POST", u.String(), c.buffer)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if len(body) > 0 {
		c.logger.Info("got body in response",
			zap.String("query", c.query),
			zap.String("body", string(body)),
		)
	}

	resp.Body.Close()

	c.lineCount = 0
	c.buffer.Reset()

	c.now = time.Now()
	c.nowStr = c.now.Format("2006-01-02")

	return nil
}

func (c *ClickhouseHTTPSender) SendFlatFgPB(node *fgpb.FlameGraphFlat) error {
	var err error
	c.lineCount++

	c.buffer.WriteString(strconv.FormatInt(node.Timestamp, 10))
	c.buffer.WriteString("\t")
	c.buffer.WriteString("graphite_metrics")
	c.buffer.WriteString("\t")
	c.buffer.WriteString(node.Cluster)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(node.Id, 10))
	c.buffer.WriteString("\t")
	c.buffer.WriteString(node.Name)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(node.Total, 10))
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(node.Value, 10))
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(node.ModTime, 10))
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(node.ParentID, 10))
	c.buffer.WriteString("\t")
	ids := []byte("[")
	for i, id := range node.ChildrenIds {
		if i != 0 {
			ids = append(ids, ',')
		}
		ids = append(ids, []byte(strconv.FormatInt(id, 10))...)
	}
	ids = append(ids, ']')
	c.buffer.Write(ids)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(node.Level, 10))
	c.buffer.WriteString("\t")
	c.buffer.WriteString(node.Server)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(c.nowStr)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(c.now.Unix(), 10))
	c.buffer.WriteString("\n")

	if c.lineCount > 1000000 {
		err = c.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *ClickhouseHTTPSender) SendMetricstatsHTTP(node *fgpb.FlatMetricInfo) error {
	var err error
	c.lineCount++

	c.buffer.WriteString(strconv.FormatInt(node.Timestamp, 10))
	c.buffer.WriteString("\t")
	c.buffer.WriteString("graphite_metrics")
	c.buffer.WriteString("\t")
	c.buffer.WriteString(node.Cluster)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(node.Path)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(node.ModTime, 10))
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(node.ATime, 10))
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(node.RdTime, 10))
	c.buffer.WriteString("\t")
	c.buffer.WriteString(node.Server)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(c.nowStr)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(c.now.Unix(), 10))
	c.buffer.WriteString("\n")

	if c.lineCount > 1000000 {
		err = c.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *ClickhouseHTTPSender) SendClusterHTTP(cluster, server string) error {
	var err error
	c.lineCount++

	c.buffer.WriteString("graphite_metrics")
	c.buffer.WriteString("\t")
	c.buffer.WriteString(cluster)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(c.nowStr)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(server)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(c.now.Unix(), 10))
	c.buffer.WriteString("\n")

	if c.lineCount > 1000000 {
		err = c.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *ClickhouseHTTPSender) SendTimestampHTTP(graphType, cluster string, timestamp int64) error {
	var err error
	nowhere := time.Unix(0, 0).Format("2006-01-02")
	c.lineCount++

	c.buffer.WriteString(graphType)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(cluster)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(timestamp, 10))
	c.buffer.WriteString("\t")
	c.buffer.WriteString(nowhere)
	c.buffer.WriteString("\t")
	c.buffer.WriteString(strconv.FormatInt(c.now.Unix(), 10))
	c.buffer.WriteString("\n")

	if c.lineCount > 1000000 {
		err = c.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

const (
	FlamegraphInsertHttpQuery = "INSERT INTO flamegraph (timestamp, type, cluster, id, name, total, value, mtime, parent_id, children_ids, level, server, date, version)"
	FlamegraphInsertQuery = "INSERT INTO flamegraph (timestamp, type, cluster, id, name, total, value, mtime, parent_id, children_ids, level, server, date, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	BaseLevel             = 0
)

var ErrGracefulCommitNotEnoughLines = fmt.Errorf("not enough lines to commit")

func (c *ClickhouseSender) GracefulCommit() error {
	if c.lines < 100000 {
		return ErrGracefulCommitNotEnoughLines
	}
	err := c.tx.Commit()
	if err != nil {
		return err
	}
	c.tx, c.stmt, err = DBStartTransaction(c.db, c.query)
	if err != nil {
		return err
	}
	c.commitedLines += int64(c.lines)
	c.lines = 0
	c.txStart = time.Now()
	return nil
}

func (c *ClickhouseSender) SendFlatFgPB(node *fgpb.FlameGraphFlat) error {
	c.lines++

	_, err := c.stmt.Exec(
		node.Timestamp,
		"graphite_metrics",
		node.Cluster,
		node.Id,
		node.Name,
		node.Total,
		node.Value,
		node.ModTime,
		node.ParentID,
		clickhouse.Array(node.ChildrenIds),
		node.Level,
		node.Server,
		c.now,
		uint64(c.now.Unix()),
	)
	if err != nil {
		return err
	}

	if c.lines >= c.linesToBuffer || time.Since(c.txStart) > 2*time.Second {
		err = c.tx.Commit()
		if err != nil {
			return err
		}
		c.tx, c.stmt, err = DBStartTransaction(c.db, c.query)
		if err != nil {
			return err
		}
		c.commitedLines += int64(c.lines)
		c.lines = 0
		c.txStart = time.Now()
	}

	return nil
}

const (
	FlamegraphClusterInsertHTTPQuery = "INSERT INTO flamegraph_clusters (type, cluster, date, server, version)"
	FlamegraphClusterInsertQuery = "INSERT INTO flamegraph_clusters (type, cluster, date, server, version) VALUES (?, ?, ?, ?, ?)"
)

func (c *ClickhouseSender) SendCluster(cluster, server string) error {
	tx, stmt, err := DBStartTransaction(c.db, FlamegraphClusterInsertQuery)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		"graphite_metrics",
		cluster,
		c.now,
		server,
		uint64(c.now.Unix()),
	)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (c *ClickhouseSender) SendFgPB(tree *fgpb.FlameGraph) error {
	tx, stmt, err := DBStartTransaction(c.db, FlamegraphInsertQuery)
	if err != nil {
		return err
	}

	err = c.sendFgNode(stmt, BaseLevel, tree.Timestamp, tree.Cluster, tree.Server, tree.Tree)
	if err != nil {
		return err
	}

	tx.Commit()

	return c.SendTimestamp("flamegraph", tree.Cluster, tree.Timestamp)
}

func (c *ClickhouseSender) sendFgNode(stmt *sql.Stmt, level int, timestamp int64, cluster, server string, node *fgpb.FlameGraphNode) error {
	c.lines++

	_, err := stmt.Exec(
		timestamp,
		"graphite_metrics",
		cluster,
		node.Id,
		node.Name,
		node.Total,
		node.Value,
		node.ModTime,
		node.ParentID,
		clickhouse.Array(node.ChildrenIds),
		level,
		server,
		c.now,
		uint64(c.now.Unix()),
	)
	if err != nil {
		return err
	}

	if node.Children != nil {
		level += 1
		for _, childNode := range node.Children {
			c.sendFgNode(stmt, level, timestamp, cluster, server, childNode)
		}
	}

	return err
}

func (c *ClickhouseSender) SendFg(cluster, name string, id int64, mtime int64, total, value, parentID int64, childrenIds []int64, level uint64) error {
	c.lines++

	_, err := c.stmt.Exec(
		c.version,
		"graphite_metrics",
		cluster,
		id,
		name,
		total,
		value,
		parentID,
		clickhouse.Array(childrenIds),
		level,
		mtime,
		c.now,
		uint64(c.version),
	)
	if err != nil {
		return err
	}

	if c.lines >= c.linesToBuffer || time.Since(c.txStart) > 280*time.Second {
		err = c.tx.Commit()
		if err != nil {
			return err
		}
		c.tx, c.stmt, err = DBStartTransaction(c.db, FlamegraphInsertQuery)
		if err != nil {
			return err
		}
		c.commitedLines += int64(c.lines)
		c.lines = 0
		c.txStart = time.Now()
	}

	return err
}

const (
	FlamegraphTimestampInsertHTTPQuery = "INSERT INTO flamegraph_timestamps (type, cluster, timestamp, date, version)"
	FlamegraphTimestampInsertQuery = "INSERT INTO flamegraph_timestamps (type, cluster, timestamp, date, version) VALUES (?, ?, ?, ?, ?)"
)

func (c *ClickhouseSender) SendTimestamp(graphType, cluster string, timestamp int64) error {
	tx, stmt, err := DBStartTransaction(c.db, FlamegraphTimestampInsertQuery)
	if err != nil {
		return err
	}

	nowhere := time.Unix(0, 0)
	now := time.Now()
	_, err = stmt.Exec(
		graphType,
		cluster,
		timestamp,
		nowhere,
		uint64(now.Unix()),
	)
	if err != nil {
		return err
	}

	return tx.Commit()
}

const (
	MetricStatInsertHTTPQuery = "INSERT INTO metricstats (timestamp, type, cluster, name, mtime, atime, rdtime, server, date, version)"

	MetricStatInsertQuery = "INSERT INTO metricstats (timestamp, type, cluster, name, mtime, atime, rdtime, server, date, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

func (c *ClickhouseSender) SendFlatMetricStatsPB(stats *fgpb.FlatMetricInfo) error {
	c.lines++
	_, err := c.stmt.Exec(
		stats.Timestamp,
		"graphite_metrics",
		stats.Cluster,
		stats.Path,
		stats.ModTime,
		stats.ATime,
		stats.RdTime,
		stats.Server,
		c.now,
		uint64(c.now.Unix()),
	)
	if err != nil {
		return err
	}

	return nil
}

func (c *ClickhouseSender) SendMetricStatsPB(stats *fgpb.MultiMetricStats) error {
	tx, stmt, err := DBStartTransaction(c.db, MetricStatInsertQuery)
	if err != nil {
		return err
	}

	for _, s := range stats.Metrics {
		c.lines++
		_, err := stmt.Exec(
			stats.Timestamp,
			"graphite_metrics",
			stats.Cluster,
			s.Path,
			s.ModTime,
			s.ATime,
			s.RdTime,
			stats.Server,
			c.now,
			uint64(c.now.Unix()),
		)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return c.SendTimestamp("metricstats", stats.Cluster, stats.Timestamp)

}

func (c *ClickhouseSender) SendMetricStats(timestamp int64, cluster, path string, mtime, atime, rdtime, count int64) error {
	c.lines++
	_, err := c.stmt.Exec(
		timestamp,
		"graphite_metrics",
		cluster,
		path,
		mtime,
		atime,
		rdtime,
		count,
		c.now,
		uint64(timestamp),
	)
	if err != nil {
		return err
	}

	if c.lines >= c.linesToBuffer || time.Since(c.txStart) > 280*time.Second {
		err = c.tx.Commit()
		if err != nil {
			return err
		}
		c.tx, c.stmt, err = DBStartTransaction(c.db, MetricStatInsertQuery)
		if err != nil {
			return err
		}
		c.commitedLines += int64(c.lines)
		c.lines = 0
		c.txStart = time.Now()
	}
	return err
}

func (c *ClickhouseSender) Commit() (int64, error) {
	if c.tx == nil {
		return 0, fmt.Errorf("no transaction running")
	}
	c.commitedLines += int64(c.lines)
	return c.commitedLines, c.tx.Commit()
}

func (c *ClickhouseSender) CommitAndRenew(tx *sql.Tx, query string) error {
	err := tx.Commit()
	if err != nil {
		return err
	}
	c.tx, c.stmt, err = DBStartTransaction(c.db, query)
	if err != nil {
		return err
	}
	c.commitedLines += int64(c.lines)
	c.lines = 0
	c.txStart = time.Now()
	return nil
}
