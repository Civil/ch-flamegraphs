package main

import (
	"strings"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	"database/sql"
)

func getClusters(db *sql.DB) ([]string, error) {
	if err := db.Ping(); err != nil {
		return nil, err
	}

	query := "select groupUniqArray(cluster) from flamegraph_clusters where type='graphite_metrics'"

	var resp []string
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var v []string
		err = rows.Scan(&v)
		if err != nil {
			return nil, err
		}
		resp = append(resp, v...)
	}

	return resp, nil
}

const (
	schema_version uint64 = 1
)

// (type, cluster, timestamp, date
func createTimestampsTable(db *sql.DB, tablePostfix, engine string) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS flamegraph_timestamps" + tablePostfix + ` (
			type String,
			cluster String,
			server String,
			timestamp Int64,
			date Date,
			count Int64 DEFAULT 1,
			version UInt64 DEFAULT 0
		) engine=` + engine)
	if err != nil {
		return err
	}

	if strings.HasPrefix(engine, "Distributed") {
		return nil
	}

	return nil
}

func createMetricStatsTable(db *sql.DB, tablePostfix, engine string) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS metricstats" + tablePostfix + ` (
			timestamp Int64,
			type String,
			cluster String,
	        server String,
			name String,
			mtime Int64,
			atime Int64,
			rdtime Int64,
			date Date,
			version UInt64 DEFAULT 0
		) engine=` + engine)
	if err != nil {
		return err
	}

	return nil
}


func createFlameGraphTable(db *sql.DB, tablePostfix, engine string) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS flamegraph" + tablePostfix + ` (
			timestamp Int64,
			type String,
			cluster String,
			server String,
			id UInt64,
			name String,
			total UInt64,
			value UInt64,
			parent_id UInt64,
			children_ids Array(UInt64),
			level UInt64,
			date Date,
			mtime Int64,
			version UInt64 DEFAULT 0
		) engine=` + engine)
	if err != nil {
		return err
	}

	return nil
}


func createFlameGraphClusterTable(db *sql.DB, tablePostfix, engine string) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS flamegraph_clusters" + tablePostfix + ` (
			type String,
			cluster String,
			date Date,
			server String,
			version UInt64 DEFAULT 0
		) engine=` + engine)
	if err != nil {
		return err
	}

	return nil
}

func createLocalTables(db *sql.DB, tablePostfix string) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS flamegraph_table_version_local (
			schema_version UInt64,
			date Date,
			version UInt64
		) engine=ReplacingMergeTree(date, (schema_version, date), 8192, version)
	`)

	if err != nil {
		return err
	}

	err = createTimestampsTable(db, tablePostfix, "MergeTree(date, (type, cluster, timestamp, server, date), 8192)")
	if err != nil {
		return err
	}

	err = createMetricStatsTable(db, tablePostfix, "MergeTree(date, (timestamp, cluster, type, mtime, atime, rdtime, name, server, date), 8192)")
	if err != nil {
		return err
	}

	err = createFlameGraphTable(db, tablePostfix, "MergeTree(date, (timestamp, cluster, type, id, parent_id, level, value, name, mtime, server, date), 8192)")
	if err != nil {
		return err
	}

	err = createFlameGraphClusterTable(db, tablePostfix, "MergeTree(date, (type, cluster, date), 8192)")

	return err
}

func createDistributedTables(db *sql.DB) error {
	err := createTimestampsTable(db, "", "Distributed(flamegraph, 'default', 'flamegraph_timestamps_local', timestamp)")
	if err != nil {
		return err
	}

	err = createMetricStatsTable(db, "", "Distributed(flamegraph, 'default', 'metricstats_local', sipHash64(name))")
	if err != nil {
		return err
	}

	err = createFlameGraphTable(db, "", "Distributed(flamegraph, 'default', 'flamegraph_local', sipHash64(name))")
	if err != nil {
		return err
	}

	err = createFlameGraphClusterTable(db, "", "Distributed(flamegraph, 'default', 'flamegraph_clusters_local', sipHash64(cluster))")
	return err
}

func migrateOrCreateTables(db *sql.DB) {
	logger := zapwriter.Logger("migrations")
	tablePostfix := ""
	if config.Clickhouse.UseDistributedTables {
		tablePostfix = "_local"
	}

	err := createLocalTables(db, tablePostfix)
	if err != nil {
		logger.Fatal("failed to create tables",
			zap.Error(err),
		)

	}

	if config.Clickhouse.UseDistributedTables {
		err := createDistributedTables(db)
		if err != nil {
			logger.Fatal("failed to create tables",
				zap.Error(err),
			)
		}
	}

	// Check version of the table schema if any version is present

	rows, err := db.Query("SELECT max(schema_version) FROM flamegraph_table_version_local")
	if err != nil {
		logger.Fatal("Error during database query",
			zap.Error(err),
		)
	}
	version := uint64(0)
	for rows.Next() {
		err = rows.Scan(&version)
		if err != nil {
			logger.Warn("Error getting version",
				zap.Error(err),
			)
		}
	}

	if version != schema_version {
		date := time.Unix(1, 0)
		versionDb := uint64(time.Now().Unix())

		tx, err := db.Begin()
		if err != nil {
			logger.Fatal("Error updating version",
				zap.Error(err),
			)
		}

		stmt, err := tx.Prepare("INSERT INTO flamegraph_table_version_local (schema_version, date, version) VALUES (?, ?, ?)")
		if err != nil {
			logger.Fatal("Error updating version",
				zap.Error(err),
			)
		}

		_, err = stmt.Exec(
			schema_version,
			date,
			versionDb,
		)
		if err != nil {
			logger.Fatal("Error updating version",
				zap.Error(err),
			)
		}

		err = tx.Commit()
		if err != nil {
			logger.Fatal("Error updating version",
				zap.Error(err),
			)
		}
	}
}
