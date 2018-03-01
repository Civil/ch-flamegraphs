package main

import (
	"encoding/json"
	"flag"
	"fmt"

	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"database/sql"

	"github.com/Civil/ch-flamegraphs/helper"
	"github.com/Civil/ch-flamegraphs/types"
	ecache "github.com/dgryski/go-expirecache"
	pb "github.com/go-graphite/carbonzipper/carbonzipperpb3"
	"github.com/kshvakov/clickhouse"
)

var logger *zap.Logger

// Copied from github.com/dgryski/carbonapi

type limiter chan struct{}

func (l limiter) enter() { l <- struct{}{} }
func (l limiter) leave() { <-l }

func newLimiter(l int) limiter {
	return make(chan struct{}, l)
}

// End of copy from carbonapi

func constructTree(root *types.FlameGraphNode, details *pb.MetricDetailsResponse) {
	cnt := types.RootElementId + 2
	total := uint64(details.TotalSpace)
	occupiedByMetrics := uint64(0)
	seen := make(map[string]*types.FlameGraphNode)
	var seenSoFar string
	var seenSoFarPrev string

	for metric, data := range details.Metrics {
		occupiedByMetrics += uint64(data.Size_)
		seenSoFar = ""
		parts := strings.Split(metric, ".")
		l := len(parts) - 1
		for i, part := range parts {
			if part == "" {
				continue
			}
			seenSoFarPrev = seenSoFar
			seenSoFar = seenSoFar + "." + part
			if n, ok := seen[seenSoFar]; ok {
				n.Count++
				n.Value += uint64(data.Size_)
				if n.ModTime < data.ModTime {
					n.ModTime = data.ModTime
				}
				if n.ATime < data.ATime {
					n.ATime = data.ATime
				}
				if n.RdTime < data.RdTime {
					n.RdTime = data.RdTime
				}
			} else {
				var parent *types.FlameGraphNode
				if seenSoFarPrev != "" {
					parent = seen[seenSoFarPrev]
				} else {
					parent = root
				}

				v := uint64(0)
				if i == l {
					v = uint64(data.Size_)
				}

				m := &types.FlameGraphNode{
					Id:      cnt,
					Cluster: parent.Cluster,
					Name:    part,
					Value:   v,
					ModTime: data.ModTime,
					RdTime:  data.RdTime,
					ATime:   data.ATime,
					Total:   total,
					Parent:  parent,
				}
				seen[seenSoFar] = m
				parent.Children = append(parent.Children, m)
				parent.ChildrenIds = append(parent.ChildrenIds, cnt)
				cnt++
			}
		}
	}

	if occupiedByMetrics+details.FreeSpace < total {
		occupiedByRest := total - occupiedByMetrics - details.FreeSpace
		m := &types.FlameGraphNode{
			Id:      cnt,
			Cluster: root.Cluster,
			Name:    "[not-whisper]",
			Value:   occupiedByRest,
			ModTime: root.ModTime,
			Total:   total,
			Parent:  root,
		}

		root.ChildrenIds = append(root.ChildrenIds, cnt)
		root.Children = append(root.Children, m)
	} else {
		logger.Error("occupiedByMetrics > totalSpace-freeSpace",
			zap.String("cluster", root.Cluster),
			zap.Uint64("occupied_by_metrics", occupiedByMetrics),
			zap.Uint64("free_space", details.FreeSpace),
			zap.Uint64("total_space", details.TotalSpace),
		)
	}
}

func updateKnownClusters(clusters []string) error {
	clusterDate := time.Unix(1, 0)
	version := uint64(time.Now().Unix())

	tx, stmt, err := helper.DBStartTransaction(config.db, "INSERT INTO new_flamegraph_clusters (graph_type, cluster, date, version) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}

	for _, c := range clusters {
		_, err := stmt.Exec(
			"graphite_metrics",
			c,
			clusterDate,
			version,
		)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func updateTimestamps(clusters []types.Cluster, t int64) error {
	logger.Info("Sending timestamps to clickhouse")
	now := time.Now()

	tx, stmt, err := helper.DBStartTransaction(config.db, "INSERT INTO new_flamegraph_timestamps (graph_type, cluster, timestamp, date) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}

	for _, cluster := range clusters {
		_, err := stmt.Exec(
			"graphite_metrics",
			cluster.Name,
			t,
			now,
		)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func sendMetricsStatsToClickhouse(stats *pb.MetricDetailsResponse, t int64, cluster string) {
	logger := logger.With(
		zap.String("cluster", cluster),
	)
	logger.Info("Sending metrics stats to clickhouse",
		zap.String("cluster", cluster),
	)

	sender, err := helper.NewClickhouseSender(config.db, "INSERT INTO new_metricstats (timestamp, graph_type, cluster, id, name, mtime, atime, rdtime, count, date, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", t, config.RowsPerInsert)
	if err != nil {
		logger.Error("failed to initialize sender",
			zap.Error(err),
		)
		return
	}

	id := uint64(0)
	for path, data := range stats.Metrics {
		id++
		err = sender.SendMetricStats(cluster, path, id, data.ModTime, data.ATime, data.RdTime, 1)
		if err != nil {
			logger.Error("failed to execute statement",
				zap.Error(err),
			)
			return
		}
	}

	lines, err := sender.Commit()
	if err != nil {
		logger.Error("failed to commit",
			zap.Error(err),
		)
		return
	}
	logger.Info("metrics stats written",
		zap.String("cluster", cluster),
		zap.Int64("lines", lines),
	)
}

func convertAndSendToClickhouse(sender *helper.ClickhouseSender, node *types.FlameGraphNode, level uint64) error {
	parentID := uint64(0)
	if node.Parent != nil {
		parentID = node.Parent.Id
	}
	err := sender.SendFg(node.Cluster, node.Name, node.Id, node.ModTime, node.Total, node.Value, parentID, node.ChildrenIds, level)
	if err != nil {
		return err
	}
	level++
	for _, n := range node.Children {
		err = convertAndSendToClickhouse(sender, n, level)
		if err != nil {
			return err
		}
	}
	return nil
}

func sendToClickhouse(node *types.FlameGraphNode, t int64) {
	logger := logger.With(
		zap.String("cluster", node.Cluster),
	)
	logger.Info("Sending results to clickhouse")

	sender, err := helper.NewClickhouseSender(config.db, "INSERT INTO flamegraph (timestamp, graph_type, cluster, id, name, total, value, parent_id, children_ids, level, mtime, date, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", t, config.RowsPerInsert)
	if err != nil {
		logger.Error("failed to initialize sender",
			zap.Error(err),
		)
		return
	}

	err = convertAndSendToClickhouse(sender, node, 0)

	if err != nil {
		logger.Fatal("failed to send data to ClickHouse",
			zap.Error(err),
		)
		return
	}
	lines, err := sender.Commit()
	if err != nil {
		logger.Fatal("failed to commit data to ClickHouse",
			zap.Error(err),
		)
		return
	}
	logger.Info("sucessfuly sent data",
		zap.Int64("lines", lines),
		zap.String("cluster", node.Cluster),
	)
}

var errTimeout = fmt.Errorf("max tries exceeded")

func fetchData(httpClient *http.Client, url string) (*pb.MetricDetailsResponse, error) {
	var metricsResponse pb.MetricDetailsResponse
	var response *http.Response
	var err error
	tries := 1

retry:
	if tries > 3 {
		logger.Error("Tries exceeded while trying to fetch data",
			zap.String("url", url),
			zap.Int("try", tries),
		)
		return nil, errTimeout
	}
	response, err = httpClient.Get(url)
	if err != nil {
		logger.Error("Error during communication with client",
			zap.String("url", url),
			zap.Int("try", tries),
			zap.Error(err),
		)
		tries++
		time.Sleep(300 * time.Millisecond)
		goto retry
	} else {
		defer response.Body.Close()
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			logger.Error("Error while reading client's response",
				zap.String("url", url),
				zap.Int("try", tries),
				zap.Error(err),
			)
			tries++
			time.Sleep(300 * time.Millisecond)
			goto retry
		}

		err = metricsResponse.Unmarshal(body)
		if err != nil || len(metricsResponse.Metrics) == 0 {
			logger.Error("Error while parsing client's response",
				zap.String("url", url),
				zap.Int("try", tries),
				zap.Error(err),
			)
			tries++
			time.Sleep(300 * time.Millisecond)
			goto retry
		}
	}

	return &metricsResponse, nil
}

type details struct {
	size    int64
	modTime int64
	count   int64
}

type metricDetails struct {
	details    map[string]details
	freeSpace  uint64
	totalSpace uint64
}

func getDetails(ips []string, cluster string) *pb.MetricDetailsResponse {
	httpClient := &http.Client{Timeout: 120 * time.Second}
	response := &pb.MetricDetailsResponse{
		Metrics: make(map[string]*pb.MetricDetails),
	}
	responses := make([]*pb.MetricDetailsResponse, len(ips))
	fetchingLimiter := newLimiter(config.FetchPerCluster)

	var wg sync.WaitGroup
	for idx, ip := range ips {
		wg.Add(1)
		go func(i int, ip string) {
			fetchingLimiter.enter()
			defer fetchingLimiter.leave()
			defer wg.Done()
			url := "http://" + ip + ":8080/metrics/details/?format=protobuf"
			data, err := fetchData(httpClient, url)
			if err != nil {
				logger.Error("timeout during fetching details",
					zap.String("host", ip),
				)
				return
			}
			responses[i] = data
		}(idx, ip)
	}
	wg.Wait()

	maxCount := uint64(1)
	metricsReplicationCounter := make(map[string]uint64)
	for idx := range responses {
		if responses[idx] == nil {
			continue
		}

		response.FreeSpace += responses[idx].FreeSpace
		response.TotalSpace += responses[idx].TotalSpace

		for m, v := range responses[idx].Metrics {
			if r, ok := response.Metrics[m]; ok {
				metricsReplicationCounter[m]++
				if metricsReplicationCounter[m] > maxCount {
					maxCount = metricsReplicationCounter[m]
				}
				if v.ModTime > r.ModTime {
					r.ModTime = v.ModTime
				}
				if v.Size_ > r.Size_ {
					r.Size_ = v.Size_
				}
			} else {
				response.Metrics[m] = v
			}
		}
	}

	response.FreeSpace /= uint64(maxCount)
	response.TotalSpace /= uint64(maxCount)

	return response
}

func parseTree(cluster *types.Cluster, t int64) {
	t0 := time.Now()
	defer func() {
		if r := recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("Unknown error")
			}
			logger.Error("panic constructing tree",
				zap.String("cluster", cluster.Name),
				zap.Error(err),
				zap.Stack("stack"),
			)
		}
	}()
	details := getDetails(cluster.Hosts, cluster.Name)
	if details == nil {
		logger.Error("failed to parse tree",
			zap.String("cluster", cluster.Name),
			zap.Strings("hosts", cluster.Hosts),
		)
		return
	}

	logger.Info("Got results",
		zap.String("cluster", cluster.Name),
		zap.Int("metrics", len(details.Metrics)),
	)

	if !config.DryRun {
		sendMetricsStatsToClickhouse(details, t, cluster.Name)
	}

	flameGraphTreeRoot := &types.FlameGraphNode{
		Id:      types.RootElementId,
		Cluster: cluster.Name,
		Name:    "[disk]",
		Value:   0,
		Total:   details.TotalSpace,
		Parent:  nil,
	}

	freeSpaceNode := &types.FlameGraphNode{
		Id:      types.RootElementId + 1,
		Cluster: cluster.Name,
		Name:    "[free]",
		Value:   details.FreeSpace,
		Total:   details.TotalSpace,
		Parent:  flameGraphTreeRoot,
	}

	flameGraphTreeRoot.ChildrenIds = append(flameGraphTreeRoot.ChildrenIds, types.RootElementId+1)
	flameGraphTreeRoot.Children = append(flameGraphTreeRoot.Children, freeSpaceNode)

	constructTree(flameGraphTreeRoot, details)

	flameGraphTreeRoot.Value = details.TotalSpace

	// Convert to clickhouse format
	if !config.DryRun {
		sendToClickhouse(flameGraphTreeRoot, t)
	} else {
		data, err := json.Marshal(flameGraphTreeRoot)
		if err != nil {
			logger.Error("failed to marshal data to json",
				zap.Error(err),
			)
		} else {
			fmt.Printf("%v\b", string(data))
		}
	}

	logger.Info("Finished generating graphs",
		zap.String("cluster", cluster.Name),
		zap.Duration("cluster_processing_time_seconds", time.Since(t0)),
	)
}

func processData() {
	clusterLimiter := newLimiter(config.ClustersInParallel)
	for {
		t0 := time.Now()
		logger.Info("Iteration start")

		var wg sync.WaitGroup
		clusters := int32(0)
		for idx := range config.Clusters {
			clusterLimiter.enter()
			cluster := &config.Clusters[idx]
			wg.Add(1)
			logger.Info("Fetching results",
				zap.Any("cluster", cluster),
			)

			go func(t int64) {
				parseTree(cluster, t)
				clusterLimiter.leave()
				wg.Done()
				atomic.AddInt32(&clusters, -1)
				if config.MemoryProfile != "" {
					f, err := os.Create(config.MemoryProfile + "." + cluster.Name)
					if err != nil {
						logger.Error("cannot create memory profile",
							zap.Error(err),
						)
						return
					}
					pprof.WriteHeapProfile(f)
					f.Close()
				}
			}(t0.Unix())
		}
		wg.Wait()

		if !config.DryRun {
			err := updateTimestamps(config.Clusters, t0.Unix())
			if err != nil {
				logger.Error("failed to update timestamps",
					zap.Error(err),
				)
			}
		}

		spentTime := time.Since(t0)
		sleepTime := config.RerunInterval - spentTime
		logger.Info("All work is done!",
			zap.Duration("total_processing_time_seconds", spentTime),
			zap.Duration("sleep_time", sleepTime),
		)
		time.Sleep(sleepTime)
	}
}

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

var config = struct {
	ClustersInParallel  int
	FetchPerCluster     int
	RemoveLowestPct     float64
	RerunInterval       time.Duration
	Clusters            []types.Cluster
	DryRun              bool
	ClickhouseHost      string
	Listen              string
	CacheSize           uint64
	CacheTimeoutSeconds int32
	RowsPerInsert       int

	MemoryProfile string

	UseDistributedTables   bool
	DistributedClusterName string

	queryCache expireCache
	db         *sql.DB
}{
	ClustersInParallel:  2,
	FetchPerCluster:     4,
	RerunInterval:       10 * time.Minute,
	DryRun:              false,
	ClickhouseHost:      "tcp://127.0.0.1:9000?debug=false",
	Listen:              "[::]:8088",
	CacheSize:           0,
	CacheTimeoutSeconds: 60,
	MemoryProfile:       "",
	RowsPerInsert:       100000,

	UseDistributedTables:   true,
	DistributedClusterName: "flamegraph",
}

func getClusters() ([]string, error) {
	if err := config.db.Ping(); err != nil {
		return nil, err
	}

	query := "select groupUniqArray(cluster) from new_flamegraph_clusters where graph_type='graphite_metrics'"

	var resp []string
	rows, err := config.db.Query(query)
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

// (graph_type, cluster, timestamp, date
func createTimestampsTable(tablePostfix, engine string) error {
	_, err := config.db.Exec("CREATE TABLE IF NOT EXISTS new_flamegraph_timestamps" + tablePostfix + ` (
			graph_type String,
			cluster String,
			timestamp Int64,
			date Date,
			version UInt64 DEFAULT 0
		) engine=` + engine)

	return err
}

func createMetricStatsTable(tablePostfix, engine string) error {
	_, err := config.db.Exec("CREATE TABLE IF NOT EXISTS new_metricstats" + tablePostfix + ` (
			timestamp Int64,
			graph_type String,
			cluster String,
			id UInt64,
			name String,
			mtime Int64,
			atime Int64,
			rdtime Int64,
			count Int64,
			date Date,
			version UInt64 DEFAULT 0
		) engine=` + engine)

	return err
}

func createFlameGraphTable(tablePostfix, engine string) error {
	_, err := config.db.Exec("CREATE TABLE IF NOT EXISTS new_flamegraph" + tablePostfix + ` (
			timestamp Int64,
			graph_type String,
			cluster String,
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

	return err
}

func createFlameGraphClusterTable(tablePostfix, engine string) error {
	_, err := config.db.Exec("CREATE TABLE IF NOT EXISTS new_flamegraph_clusters" + tablePostfix + ` (
			graph_type String,
			cluster String,
			date Date,
			version UInt64 DEFAULT 0
		) engine=` + engine)

	return err
}

func createLocalTables(tablePostfix string) error {
	_, err := config.db.Exec(`
		CREATE TABLE IF NOT EXISTS new_flamegraph_table_version_local (
			schema_version UInt64,
			date Date,
			version UInt64
		) engine=ReplacingMergeTree(date, (schema_version, date), 8192, version)
	`)

	if err != nil {
		return err
	}

	err = createTimestampsTable(tablePostfix, "MergeTree(date, (graph_type, cluster, timestamp, date), 8192)")
	if err != nil {
		return err
	}

	err = createMetricStatsTable(tablePostfix, "MergeTree(date, (timestamp, graph_type, cluster, mtime, atime, rdtime, id, name, date), 8192)")
	if err != nil {
		return err
	}

	err = createFlameGraphTable(tablePostfix, "MergeTree(date, (timestamp, graph_type, cluster, id, parent_id, date, level, value, name), 8192)")
	if err != nil {
		return err
	}

	err = createFlameGraphClusterTable(tablePostfix, "MergeTree(date, (graph_type, cluster, date), 8192)")
	return err
}

func createDistributedTables() error {
	err := createTimestampsTable("", "Distributed(flamegraph, 'default', 'new_flamegraph_timestamps_local', timestamp)")
	if err != nil {
		return err
	}

	err = createMetricStatsTable("", "Distributed(flamegraph, 'default', 'new_metricstats_local', sipHash64(name))")
	if err != nil {
		return err
	}

	err = createFlameGraphTable("", "Distributed(flamegraph, 'default', 'new_flamegraph_local', sipHash64(name))")
	if err != nil {
		return err
	}

	err = createFlameGraphClusterTable("", "Distributed(flamegraph, 'default', 'new_flamegraph_clusters_local', sipHash64(cluster))")
	return err
}

func migrateOrCreateTables() {
	tablePostfix := ""
	if config.UseDistributedTables {
		tablePostfix = "_local"
	}

	err := createLocalTables(tablePostfix)
	if err != nil {
		logger.Fatal("failed to create tables",
			zap.Error(err),
		)

	}

	if config.UseDistributedTables {
		err := createDistributedTables()
		if err != nil {
			logger.Fatal("failed to create tables",
				zap.Error(err),
			)
		}
	}

	// Check version of the table schema if any version is present

	rows, err := config.db.Query("SELECT max(schema_version) FROM new_flamegraph_table_version_local")
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

		tx, err := config.db.Begin()
		if err != nil {
			logger.Fatal("Error updating version",
				zap.Error(err),
			)
		}

		stmt, err := tx.Prepare("INSERT INTO new_flamegraph_table_version_local (schema_version, date, version) VALUES (?, ?, ?)")
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
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		fmt.Printf("Error creating logger: %+v\n", err)
		os.Exit(1)
	}

	cfgPath := flag.String("config", "config.yaml", "path to the config file")
	flag.Parse()

	configRaw, err := ioutil.ReadFile(*cfgPath)
	if err != nil {
		logger.Fatal("Error reading configfile 'config.yaml'",
			zap.Error(err),
		)
	}

	err = yaml.Unmarshal(configRaw, &config)
	if err != nil {
		logger.Fatal("Error parsing config file",
			zap.Error(err),
		)
	}

	if len(config.Clusters) == 0 {
		logger.Fatal("No clusters configured")
	}

	config.queryCache = expireCache{ec: ecache.New(config.CacheSize)}
	go config.queryCache.ec.ApproximateCleaner(10 * time.Second)

	logger.Info("Started",
		zap.Int("clusters", len(config.Clusters)),
		zap.Any("config", config),
	)

	config.db, err = sql.Open("clickhouse", config.ClickhouseHost)
	if err != nil {
		logger.Fatal("error connecting to clickhouse",
			zap.Error(err),
		)
	}

	if err = config.db.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logger.Fatal("exception while pinging clickhouse",
				zap.Int32("code", exception.Code),
				zap.String("message", exception.Message),
				zap.Any("stacktrace", exception.StackTrace),
			)
		}
		logger.Fatal("error pinging clickhouse", zap.Error(err))
	}

	migrateOrCreateTables()

	knownClusters, err := getClusters()
	if err != nil {
		logger.Fatal("Error retreiving clusters",
			zap.Error(err),
		)
		return
	}

	unknownClusters := make(map[string]bool, 0)

	for _, cluster := range config.Clusters {
		found := false
		for _, knownCluster := range knownClusters {
			if cluster.Name == knownCluster {
				found = true
				break
			}
		}
		if !found {
			unknownClusters[cluster.Name] = true
		}
	}

	if len(unknownClusters) > 0 {
		keys := make([]string, 0, len(unknownClusters))
		for k := range unknownClusters {
			keys = append(keys, k)
		}
		err = updateKnownClusters(keys)
		if err != nil {
			logger.Fatal("failed to update list of clusters",
				zap.Error(err),
			)
		}
	}

	go processData()

	http.ListenAndServe("0.0.0.0:18000", nil)
}
