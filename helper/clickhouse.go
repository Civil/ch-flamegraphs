package helper

import (
	"database/sql"
	"time"

	_ "github.com/kshvakov/clickhouse"

	"github.com/Civil/carbonserver-flamegraphs/types"
	"github.com/kshvakov/clickhouse"
)

type ClickhouseConfig struct {
	ClickhouseHost string
	RowsPerInsert  int

	UseDistributedTables   bool
	DistributedClusterName string
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

func (c *ClickhouseSender) SendStacktrace(data *types.StackFlameGraphNode) error {
	c.lines++
	parentID := int64(0)
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
		c.tx, c.stmt, err = DBStartTransaction(c.db, "INSERT INTO flamegraph (timestamp, graph_type, cluster, id, name, total, value, parent_id, children_ids, level, mtime, date, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			return err
		}
		c.commitedLines += int64(c.lines)
		c.lines = 0
		c.txStart = time.Now()
	}

	return err
}

func (c *ClickhouseSender) SendMetricStats(cluster, path string, id uint64, mtime, atime, rdtime, count int64) error {
	c.lines++
	_, err := c.stmt.Exec(
		c.version,
		"graphite_metrics",
		cluster,
		id,
		path,
		mtime,
		atime,
		rdtime,
		count,
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
		c.tx, c.stmt, err = DBStartTransaction(c.db, "INSERT INTO metricstats (timestamp, graph_type, cluster, id, name, mtime, atime, rdtime, count, date, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
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
