package main

import (
	"strings"

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
	schema_version = "v1"
)

// (type, cluster, timestamp, date
func createTimestampsTable(db *sql.DB, tablePostfix, engine, cluster string) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS flamegraph_timestamps" + tablePostfix + cluster + ` (
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

func createMetricStatsTable(db *sql.DB, tablePostfix, engine, cluster string) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS metricstats" + tablePostfix + cluster + ` (
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

func createFlameGraphTable(db *sql.DB, tablePostfix, engine, cluster string) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS flamegraph" + tablePostfix + cluster + ` (
			timestamp Int64,
			type String,
			cluster String,
			server String,
			id Int64,
			name String,
			total Int64,
			value Int64,
			parent_id Int64,
			children_ids Array(Int64),
			level Int64,
			date Date,
			mtime Int64,
			version UInt64 DEFAULT 0
		) engine=` + engine)
	if err != nil {
		return err
	}

	return nil
}

func createFlameGraphClusterTable(db *sql.DB, tablePostfix, engine, cluster string) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS flamegraph_clusters" + tablePostfix + cluster + `(
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

func createTableVersion(db *sql.DB, tablePostfix, engine, cluster string) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS flamegraph_table_version` + tablePostfix + cluster + ` (
			schema_version UInt64,
			date Date,
			version UInt64
		) engine=` + engine)

	return err
}

func createLocalTables(db *sql.DB, tablePostfix, cluster string) error {
	err := createTableVersion(db, "_"+schema_version, "MergeTree PARTITION BY toYYYYMMDD(date) ORDER BY (schema_version, date)", cluster)
	if err != nil {
		return err
	}

	err = createTimestampsTable(db, tablePostfix, "MergeTree PARTITION BY toYYYYMMDD(date) ORDER BY (type, cluster, timestamp, server, date)", cluster)
	if err != nil {
		return err
	}

	err = createMetricStatsTable(db, tablePostfix, "MergeTree PARTITION BY toYYYYMMDD(date) ORDER BY (timestamp, cluster, type, mtime, atime, rdtime, name, server, date)", cluster)
	if err != nil {
		return err
	}

	err = createFlameGraphTable(db, tablePostfix, "MergeTree PARTITION BY toYYYYMMDD(date) ORDER BY (timestamp, cluster, type, id, parent_id, level, value, name, mtime, server, date)", cluster)
	if err != nil {
		return err
	}

	err = createFlameGraphClusterTable(db, tablePostfix, "MergeTree PARTITION BY toYYYYMMDD(date) ORDER BY (type, cluster, date)", cluster)

	return err
}

func createDistributedTables(db *sql.DB, cluster string) error {
	err := createTimestampsTable(db, "", "Distributed(flamegraph, 'default', 'flamegraph_timestamps_local', timestamp)", cluster)
	if err != nil {
		return err
	}

	err = createMetricStatsTable(db, "", "Distributed(flamegraph, 'default', 'metricstats_local', sipHash64(name))", cluster)
	if err != nil {
		return err
	}

	err = createFlameGraphTable(db, "", "Distributed(flamegraph, 'default', 'flamegraph_local', sipHash64(name))", cluster)
	if err != nil {
		return err
	}

	err = createFlameGraphClusterTable(db, "", "Distributed(flamegraph, 'default', 'flamegraph_clusters_local', sipHash64(cluster))", cluster)
	return err
}

func migrateOrCreateTables(db *sql.DB) {
	logger := zapwriter.Logger("migrations")

	// Check version of the table schema if any version is present
	_, err := db.Query("DESCRIBE TABLE flamegraph_table_version_" + schema_version)
	if err == nil {
		// Table exists, that means that schema version is exaclty what we expect
		return
	}

	tablePostfix := ""
	if config.Clickhouse.UseDistributedTables {
		tablePostfix = "_local"
	}
	cluster := ""
	if config.Clickhouse.Cluster != "" {
		cluster = " ON CLUSTER " + config.Clickhouse.Cluster
	}

	err = createLocalTables(db, tablePostfix, cluster)
	if err != nil {
		logger.Fatal("failed to create tables",
			zap.Error(err),
		)

	}

	if config.Clickhouse.UseDistributedTables {
		err := createDistributedTables(db, cluster)
		if err != nil {
			logger.Fatal("failed to create tables",
				zap.Error(err),
			)
		}
	}
}
