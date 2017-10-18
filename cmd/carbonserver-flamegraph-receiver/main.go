package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	_ "net/http/pprof"
	"time"

	fgpb "github.com/Civil/carbonserver-flamegraphs/flamegraphpb"
	"github.com/Civil/carbonserver-flamegraphs/helper"
	ecache "github.com/dgryski/go-expirecache"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/kshvakov/clickhouse"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"gopkg.in/yaml.v2"
	"strings"
)

var defaultLoggerConfig = zapwriter.Config{
	Logger:           "",
	File:             "stdout",
	Level:            "debug",
	Encoding:         "json",
	EncodingTime:     "iso8601",
	EncodingDuration: "seconds",
}

var logger *zap.Logger
var errTimeout = fmt.Errorf("timeout exceeded")
var errMaxTries = fmt.Errorf("max maxTries exceeded")
var errUnknown = fmt.Errorf("unknown error")

type expireCache struct {
	ec *ecache.Cache
}

func (ec expireCache) get(k string) ([]byte, bool) {
	v, ok := ec.ec.Get(k)

	if !ok {
		return nil, false
	}

	return v.([]byte), true
}

func (ec expireCache) set(k string, v []byte, expire int32) {
	ec.ec.Set(k, v, uint64(len(v)), expire)
}

type carbonserverCollector struct {
	endpoint     string
	root         *fgpb.FlameGraphNode
	maxTries     int
	fetchTimeout time.Duration

	client  fgpb.FlamegraphV1Client
	cleanup func()

	db *sql.DB

	metricStatChan chan *fgpb.MultiMetricStats
	flamegraphChan chan *fgpb.FlameGraph
}

var emptyResponse empty.Empty

const (
	GRPCAPIVersion = 0
)

func (c carbonserverCollector) GetVersion(ctx context.Context, empty *empty.Empty) (*fgpb.ProtocolVersionResponse, error) {
	t0 := time.Now()
	logger := zapwriter.Logger("access").With(
		zap.String("handler", "GetVersion"),
	)
	resp := &fgpb.ProtocolVersionResponse{Version: GRPCAPIVersion}
	logger.Info("request served",

		zap.Duration("runtime", time.Since(t0)),
	)
	return resp, nil

}

func (c carbonserverCollector) SendFlamegraph(ctx context.Context, in *fgpb.FlameGraph) (*empty.Empty, error) {
	t0 := time.Now()
	logger := zapwriter.Logger("access").With(
		zap.String("handler", "SendFlamegraph"),
	)
	logger.Debug("data received",
		zap.Time("current_time", time.Now()),
		zap.Int("size", in.Size()),
	)

	c.flamegraphChan <- in

	logger.Info("request served",
		zap.Duration("runtime", time.Since(t0)),
	)
	return &emptyResponse, nil
}

func (c carbonserverCollector) SendMetricsStats(ctx context.Context, in *fgpb.MultiMetricStats) (*empty.Empty, error) {
	t0 := time.Now()
	logger := zapwriter.Logger("access").With(
		zap.String("handler", "SendMetricsStats"),
	)
	logger.Debug("data received",
		zap.Time("current_time", time.Now()),
		zap.Int("size", in.Size()),
	)

	c.metricStatChan <- in

	logger.Info("request served",
		zap.Duration("runtime", time.Since(t0)),
	)
	return &emptyResponse, nil
}

func (c *carbonserverCollector) sender() {
	logger := zapwriter.Logger("sender").With(zap.String("type", "metricstat"))
	msSender, err := helper.NewClickhouseSender(c.db, helper.MetricStatInsertQuery, time.Now().Unix(), config.RowsPerInsert)
	if err != nil {
		logger.Fatal("error initializing clickhouse sender",
			zap.Error(err),
		)
	}
	fgSender, err := helper.NewClickhouseSender(c.db, helper.FlamegraphInsertQuery, time.Now().Unix(), config.RowsPerInsert)
	if err != nil {
		logger.Fatal("error initializing clickhouse sender",
			zap.Error(err),
		)
	}
	ticker := time.Tick(5 * time.Second)
	for {
		select {
		case ms := <-c.metricStatChan:
			err := msSender.SendMetricStatsPB(ms)
			if err != nil {
				logger.Error("failed to send metricstats",
					zap.Error(err),
				)
			}
			err = fgSender.SendTimestamp("metricstats", ms.Cluster, ms.Timestamp)
			if err != nil {
				logger.Error("failed to update list of available timestamps for metricstats",
					zap.Error(err),
				)
			}
		case fg := <-c.flamegraphChan:
			err := fgSender.SendFgPB(fg)
			if err != nil {
				logger.Error("failed to send flamegraph",
					zap.Error(err),
				)
				continue
			}

			err = fgSender.SendTimestamp("flamegraph", fg.Cluster, fg.Timestamp)
			if err != nil {
				logger.Error("failed to update list of available timestamps for metricstats",
					zap.Error(err),
				)
			}
		case <-ticker:
			logger.Debug("commiting by timeout")
			lines, err := msSender.CommitAndRenew()
			if err != nil {
				logger.Error("failed to commit",
					zap.Error(err),
				)
			} else {
				logger.Debug("commited to metricstats",
					zap.Int64("lines", lines),
				)
			}

			lines, err = fgSender.CommitAndRenew()
			if err != nil {
				logger.Error("failed to commit",
					zap.Error(err),
				)
			} else {
				logger.Debug("commited to flamegraph",
					zap.Int64("lines", lines),
				)
			}
		}
	}
}

func newCarbonserverCollector(db *sql.DB) (*carbonserverCollector, error) {
	collector := carbonserverCollector{
		db:             db,
		metricStatChan: make(chan *fgpb.MultiMetricStats, 1024),
		flamegraphChan: make(chan *fgpb.FlameGraph, 1024),
	}

	go collector.sender()

	return &collector, nil
}

type connectOptions struct {
	Insecure bool `yaml:"insecure"`
}

var config = struct {
	helper.ClickhouseConfig

	Listen         string         `yaml:"listen"`
	ConnectOptions connectOptions `yaml:"connect_options"`
	CacheSize      uint64

	Logger []zapwriter.Config `yaml:"logger"`

	db         *sql.DB
	queryCache expireCache
}{
	ClickhouseConfig: helper.ClickhouseConfig{
		ClickhouseHost:         "tcp://127.0.0.1:9000?debug=false",
		RowsPerInsert:          1000000,
		UseDistributedTables:   false,
		DistributedClusterName: "flamegraph",
	},
	Listen: "[::]:8088",
	ConnectOptions: connectOptions{
		Insecure: true,
	},
	CacheSize: 10000,

	Logger: []zapwriter.Config{defaultLoggerConfig},
}

func validateConfig() {
	switch {
	case config.Listen == "":
		logger.Fatal("listen can't be empty")
	case config.ClickhouseHost == "":
		logger.Fatal("clickhouse host can't be empty")
	}
}

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

	err = createTimestampsMV(db, tablePostfix)
	return err
}

func createTimestampsMV(db *sql.DB, tablePostfix string) error {
	_, err := db.Exec("CREATE MATERIALIZED VIEW IF NOT EXISTS flamegraph_timestamps" + tablePostfix + `_mv
		ENGINE = AggregatingMergeTree(date, (type, cluster, timestamp, date), 8192)
		AS SELECT
			type as type,
			cluster as cluster,
			uniqState(server) as count,
			timestamp as timestamp,
			date,
			maxState(version) as version
		FROM flamegraph_timestamps` + tablePostfix + `
		GROUP BY type, cluster, timestamp, date
		`)
	if err != nil {
		return err
	}

	_, err = db.Exec("CREATE VIEW IF NOT EXISTS flamegraph_timestamps" + tablePostfix + `_view
		AS SELECT
			type as type,
			cluster as cluster,
			uniqMerge(count) as count,
			timestamp as timestamp,
			date,
			maxMerge(version) as version
		FROM flamegraph_timestamps` + tablePostfix + `_mv
		GROUP BY type, cluster, timestamp, date
		`)

	return err
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

	if strings.HasPrefix(engine, "Distributed") {
		return nil
	}

	err = createMetricStatsMV(db, tablePostfix)
	return err
}

func createMetricStatsMV(db *sql.DB, tablePostfix string) error {
	_, err := db.Exec("CREATE MATERIALIZED VIEW IF NOT EXISTS metricstats" + tablePostfix + `_mv
		ENGINE = AggregatingMergeTree(date, (timestamp, cluster, type, mtime, atime, rdtime, name, date), 8192)
		AS SELECT
			timestamp as timestamp,
			type as type,
			cluster as cluster,
	        uniqState(server) as count,
			name as name,
			maxState(mtime) as mtime,
			maxState(atime) as atime,
			maxState(rdtime) as rdtime,
			date,
			maxState(version) as version
		FROM metricstats` + tablePostfix + `
		GROUP BY timestamp, type, cluster, name, date
		`)
	if err != nil {
		return err
	}

	_, err = db.Exec("CREATE VIEW IF NOT EXISTS metricstats" + tablePostfix + `_view
		AS SELECT
			timestamp as timestamp,
			type as type,
			cluster as cluster,
	        uniqMerge(count) as count,
			name as name,
			maxMerge(mtime) as mtime,
			maxMerge(atime) as atime,
			maxMerge(rdtime) as rdtime,
			date,
			maxMerge(version) as version
		FROM metricstats` + tablePostfix + `_mv
		GROUP BY timestamp, type, cluster, name, date
		`)

	return err
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

	if strings.HasPrefix(engine, "Distributed") {
		return nil
	}

	err = createFlameGraphMV(db, tablePostfix)
	return err
}

func createFlameGraphMV(db *sql.DB, tablePostfix string) error {
	_, err := db.Exec("CREATE MATERIALIZED VIEW IF NOT EXISTS flamegraph" + tablePostfix + `_mv
		ENGINE = AggregatingMergeTree(date, (timestamp, cluster, type, id, parent_id, level, value, name, mtime, date), 8192)
		AS SELECT
			timestamp as timestamp,
			type as type,
			cluster as cluster,
			uniqState(server) as count,
			id as id,
			name as name,
			sumState(total) as total,
			sumState(value) as value,
			parent_id as parent_id,
			groupArrayState(children_ids) as children_ids,
			anyState(level) as level,
			date,
			maxState(mtime) as mtime,
			maxState(version) as version
		FROM flamegraph` + tablePostfix + `
		GROUP BY timestamp, type, cluster, id, name, parent_id, date
		`)

	if err != nil {
		return err
	}

	_, err = db.Exec("CREATE VIEW IF NOT EXISTS flamegraph" + tablePostfix + `_view
		AS SELECT
			timestamp as timestamp,
			type as type,
			cluster as cluster,
			uniqMerge(count) as count,
			id as id,
			name as name,
			sumMerge(total) as total,
			sumMerge(value) as value,
			parent_id as parent_id,
			groupArrayMerge(children_ids) as children_ids,
			anyMerge(level) as level,
			date,
			maxMerge(mtime) as mtime,
			maxMerge(version) as version
		FROM flamegraph` + tablePostfix + `_mv
		GROUP BY timestamp, type, cluster, id, name, parent_id, date
		`)

	return err
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

	if strings.HasPrefix(engine, "Distributed") {
		return nil
	}

	err = createFlameGraphClusterMV(db, tablePostfix)
	return err
}

func createFlameGraphClusterMV(db *sql.DB, tablePostfix string) error {
	_, err := db.Exec("CREATE MATERIALIZED VIEW IF NOT EXISTS flamegraph_clusters" + tablePostfix + `_mv
		ENGINE = AggregatingMergeTree(date, (type, cluster, date), 8192)
		AS SELECT
			type as type,
			cluster as cluster,
			date,
			uniqState(server) as count,
			maxState(version)
		FROM flamegraph` + tablePostfix + `
		GROUP BY type, cluster, date
		`)
	if err != nil {
		return err
	}

	_, err = db.Exec("CREATE VIEW IF NOT EXISTS flamegraph_clusters" + tablePostfix + `_view
		ENGINE = AggregatingMergeTree(date, (type, cluster, date), 8192)
		AS SELECT
			type as type,
			cluster as cluster,
			date,
			uniqMerge(count) as count,
			maxMerge(version)
		FROM flamegraph` + tablePostfix + `_mv
		GROUP BY type, cluster, date
		`)

	return err
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
	tablePostfix := ""
	if config.UseDistributedTables {
		tablePostfix = "_local"
	}

	err := createLocalTables(db, tablePostfix)
	if err != nil {
		logger.Fatal("failed to create tables",
			zap.Error(err),
		)

	}

	if config.UseDistributedTables {
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

func main() {
	// var flameGraph flameGraphNode
	err := zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})
	if err != nil {
		log.Fatal("failed to initialize logger with default configuration")

	}
	logger = zapwriter.Logger("main")

	// TODO: Migrate to viper
	cfgPath := flag.String("config", "config.yaml", "path to the config file")
	flag.Parse()

	configRaw, err := ioutil.ReadFile(*cfgPath)
	if err != nil {
		logger.Fatal("error reading config",
			zap.String("config", *cfgPath),
			zap.Error(err),
		)
	}

	err = yaml.Unmarshal(configRaw, &config)
	if err != nil {
		logger.Fatal("error parsing config file",
			zap.String("config", *cfgPath),
			zap.Error(err),
		)
	}

	validateConfig()

	err = zapwriter.ApplyConfig(config.Logger)
	if err != nil {
		logger.Fatal("failed to apply config",
			zap.String("config", *cfgPath),
			zap.Any("logger_config", config.Logger),
			zap.Error(err),
		)
	}

	config.queryCache = expireCache{ec: ecache.New(config.CacheSize)}
	go config.queryCache.ec.ApproximateCleaner(10 * time.Second)

	tcpAddr, err := net.ResolveTCPAddr("tcp", config.Listen)
	if err != nil {
		logger.Fatal("error resolving address",
			zap.Error(err),
		)
	}
	tcpListener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		logger.Fatal("error binding to address",
			zap.Error(err),
		)
	}

	db, err := sql.Open("clickhouse", config.ClickhouseHost)
	if err != nil {
		logger.Fatal("error connecting to clickhouse",
			zap.Error(err),
		)
	}

	if err = db.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logger.Fatal("exception while pinging clickhouse",
				zap.Int32("code", exception.Code),
				zap.String("message", exception.Message),
				zap.Any("stacktrace", exception.StackTrace),
			)
		}
		logger.Fatal("error pinging clickhouse", zap.Error(err))
	}

	migrateOrCreateTables(db)

	collector, err := newCarbonserverCollector(db)
	if err != nil {
		logger.Fatal("failed to initialize collector",
			zap.Error(err),
		)
	}

	if err != nil {
		logger.Fatal("Error retreiving clusters",
			zap.Error(err),
		)
		return
	}

	grpcServer := grpc.NewServer(
		grpc.RPCDecompressor(grpc.NewGZIPDecompressor()),
		grpc.RPCCompressor(grpc.NewGZIPCompressor()),
	)
	fgpb.RegisterFlamegraphV1Server(grpcServer, collector)

	logger.Info("started",
		zap.Any("config", config),
	)

	grpcServer.Serve(tcpListener)
}
