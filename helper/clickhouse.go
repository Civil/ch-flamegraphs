package helper

import (
	"time"

	"database/sql"

	_ "github.com/kshvakov/clickhouse"

	fgpb "github.com/Civil/carbonserver-flamegraphs/flamegraphpb"
	"github.com/Civil/carbonserver-flamegraphs/types"
	"github.com/kshvakov/clickhouse"
)

type ClickhouseConfig struct {
	ClickhouseHost string `yaml:"host"`
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
	version       int64
	now           time.Time
	txStart       time.Time

	query string
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
		version:       t,
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
		c.version,
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

	parentID := uint64(0)
	if data.Parent != nil {
		parentID = data.Parent.Id
	}
	_, err := c.stmt.Exec(
		c.version,
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

const (
	FlamegraphInsertQuery = "INSERT INTO flamegraph (timestamp, type, cluster, id, name, total, value, parent_id, children_ids, level, mtime, server, date, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	BaseLevel             = 0
)

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
		node.ParentID,
		clickhouse.Array(node.ChildrenIds),
		level,
		node.ModTime,
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

func (c *ClickhouseSender) SendFg(cluster, name string, id uint64, mtime int64, total, value, parentID uint64, childrenIds []uint64, level uint64) error {
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
	MetricStatInsertQuery = "INSERT INTO metricstats (timestamp, type, cluster, name, mtime, atime, rdtime, server, date, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

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
