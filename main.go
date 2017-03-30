package main

import (
	"bufio"

	"encoding/json"
	"fmt"

	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"database/sql"

	"strconv"

	"github.com/kshvakov/clickhouse"
	ecache "github.com/dgryski/go-expirecache"
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


type flameGraphNode struct {
	id          uint64
	cluster     string
	Name        string            `json:"name""`
	Total       uint64            `json:"total"`
	Value       uint64            `json:"value""`
	Children    []*flameGraphNode `json:"children,omitempty""`
	childrenIds []uint64
	parent      *flameGraphNode
}

type metrics struct {
	Metrics []string `json:"Metrics"`
}

type Cluster struct {
	Name  string
	Hosts []string
}

var removeLowest float64

const (
	rootElementId uint64 = 1
)

func trimNodes(node *flameGraphNode, limit uint64) {
	var newChildren []*flameGraphNode
	for _, n := range node.Children {
		if n.Value > limit {
			newChildren = append(newChildren, n)
			trimNodes(n, limit)
		}
	}
	node.Children = newChildren
}

func constructTree(root *flameGraphNode, metrics []string) {
	cnt := rootElementId + 1
	seen := make(map[string]*flameGraphNode)
	total := uint64(len(metrics))
	var seenSoFar string
	var seenSoFarPrev string

	for _, metric := range metrics {
		seenSoFar = ""
		parts := strings.Split(metric, ".")
		for _, part := range parts[:len(parts)-1] {
			if part == "" {
				continue
			}
			seenSoFarPrev = seenSoFar
			seenSoFar = seenSoFar + "." + part
			if n, ok := seen[seenSoFar]; ok {
				n.Value++
			} else {
				var parent *flameGraphNode
				if seenSoFarPrev != "" {
					parent = seen[seenSoFarPrev]
				} else {
					parent = root
				}

				data := &flameGraphNode{
					id:      cnt,
					cluster: parent.cluster,
					Name:    part,
					Value:   1,
					Total:   total,
					parent:  parent,
				}
				seen[seenSoFar] = data
				parent.Children = append(parent.Children, data)
				parent.childrenIds = append(parent.childrenIds, cnt)
				cnt++
			}
		}
	}
}

type clickhouseField struct {
	Timestamp   int64
	GraphType   string
	Cluster     string
	Name        string
	Total       uint64
	Id          uint64
	Value       uint64
	ChildrenIds []uint64
}

func convertToClickhouse(node *flameGraphNode, timestamp int64) []clickhouseField {
	res := []clickhouseField{{
		Timestamp:   timestamp,
		Cluster:     node.cluster,
		Name:        node.Name,
		Total:       node.Total,
		Value:       node.Value,
		Id:          node.id,
		ChildrenIds: node.childrenIds,
	}}
	for _, n := range node.Children {
		res = append(res, clickhouseField{
			Timestamp:   timestamp,
			Cluster:     n.cluster,
			Name:        n.Name,
			Total:       n.Total,
			Value:       n.Value,
			Id:          n.id,
			ChildrenIds: n.childrenIds,
		})
		res = append(res, convertToClickhouse(n, timestamp)...)
	}
	return res
}

func sendToClickhouse(node *flameGraphNode) {
	logger := logger.With(
		zap.String("cluster", node.cluster),
	)
	logger.Info("Sending results to clickhouse")
	now := time.Now()
	t := now.Unix()

	ch := convertToClickhouse(node, t)

	connect, err := sql.Open("clickhouse", config.ClickhouseHost)
	if err != nil {
		logger.Error("error connecting to clickhouse",
			zap.Error(err),
		)
		return
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logger.Error("exception while pinging clickhouse",
				zap.Int32("code", exception.Code),
				zap.String("message", exception.Message),
				zap.Any("stacktrace", exception.StackTrace),
			)
		} else {
			logger.Error("error pinging clickhouse", zap.Error(err))
		}
		return
	}

	defer connect.Close()

	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS flamegraph (
			timestamp Int64,
			graph_type String,
			cluster String,
			id UInt64,
			name String,
			total UInt64,
			value UInt64,
			children_ids Array(UInt64),
			date Date
		) engine=MergeTree(date, (timestamp, graph_type, cluster, value, date), 8192)
	`)

	if err != nil {
		logger.Error("failed to create table",
			zap.Error(err),
		)
		return
	}

	tx, err := connect.Begin()
	if err != nil {
		logger.Error("error initializing transaction",
			zap.Error(err),
		)
		return
	}
	stmt, err := tx.Prepare("INSERT INTO flamegraph (timestamp, graph_type, cluster, id, name, total, value, children_ids, date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		logger.Error("failed to prepare the statement",
			zap.Error(err),
		)
		return
	}

	for i := range ch {
		_, err := stmt.Exec(
			t,
			"graphite_metrics",
			ch[i].Cluster,
			ch[i].Id,
			ch[i].Name,
			ch[i].Total,
			ch[i].Value,
			clickhouse.Array(ch[i].ChildrenIds),
			now,
		)
		if err != nil {
			logger.Error("failed to execute statement",
				zap.Error(err),
			)
			return
		}
	}

	err = tx.Commit()
	if err != nil {
		logger.Error("failed to commit",
			zap.Error(err),
		)
		return
	}
}

func getMetrics(ips []string) []string {
	httpClient := &http.Client{Timeout: 120 * time.Second}
	responses := make([][]string, len(ips))
	responseUniq := make(map[string]struct{})
	fetchingLimiter := newLimiter(config.FetchPerCluster)

	var wg sync.WaitGroup
	for idx, ip := range ips {
		fetchingLimiter.enter()
		wg.Add(1)
		go func() {
			defer fetchingLimiter.leave()
			defer wg.Done()
			// TODO: Move to protobuf3
			url := "http://" + ip + ":8080/metrics/list/?format=json"
			responses[idx] = getList(httpClient, url)
		}()
	}
	wg.Wait()

	for idx := range responses {
		for _, metric := range responses[idx] {
			responseUniq[metric] = struct{}{}
		}
	}

	response := make([]string, 0, len(responseUniq))
	for key := range responseUniq {
		response = append(response, key)
	}

	return response
}

func getList(httpClient *http.Client, url string) []string {
	var inputMetrics metrics
	var response *http.Response
	var err error
	tries := 1

retry:
	if tries > 3 {
		logger.Error("Tries exceeded while trying to fetch data",
			zap.String("url", url),
			zap.Int("try", tries),
		)
		return []string{}
	}
	response, err = httpClient.Get(url)
	if err != nil {
		logger.Error("Error during communication with client",
			zap.String("url", url),
			zap.Int("try", tries),
			zap.Error(err),
		)
		tries++
		goto retry
	} else {
		defer response.Body.Close()
		err = json.NewDecoder(response.Body).Decode(&inputMetrics)
		if err != nil {
			logger.Error("Error while parsing client's response",
				zap.String("url", url),
				zap.Int("try", tries),
				zap.Error(err),
			)
			tries++
			goto retry
		}
	}

	return inputMetrics.Metrics
}

func parseTree(cluster *Cluster, removeLowest float64) {
	t0 := time.Now()
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic constructing tree",
				zap.String("cluster", cluster.Name),
				zap.Stack("stack"),
			)
		}
	}()
	metrics := getMetrics(cluster.Hosts)
	logger.Info("Got results",
		zap.String("cluster", cluster.Name),
		zap.Int("metrics", len(metrics)),
	)

	flameGraphTreeRoot := &flameGraphNode{
		id:      rootElementId,
		cluster: cluster.Name,
		Name:    "all",
		Value:   uint64(len(metrics)),
		Total:   uint64(len(metrics)),
		parent:  nil,
	}
	constructTree(flameGraphTreeRoot, metrics)

	// Convert to clickhouse format
	if config.ClickhouseEnabled {
		sendToClickhouse(flameGraphTreeRoot)
	}

	if config.WriteToFile {
		// Remove everything that's small
		trimNodes(flameGraphTreeRoot, uint64(float64(len(metrics))*removeLowest))

		outFile, err := os.Create("stacks_" + cluster.Name + ".json")
		if err != nil {
			logger.Error("Failed to create output file", zap.Error(err))
		} else {
			output := bufio.NewWriter(outFile)
			enc := json.NewEncoder(output)
			if err := enc.Encode(flameGraphTreeRoot); err != nil {
				logger.Error("Error during encoding", zap.Error(err))
			}
		}
	}
	logger.Info("Finished generating graphs",
		zap.String("cluster", cluster.Name),
		zap.Duration("cluster_processing_time_seconds", time.Since(t0)),
	)
}

func processData(removeLowest float64) {
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

			go func() {
				parseTree(cluster, removeLowest)
				clusterLimiter.leave()
				wg.Done()
				atomic.AddInt32(&clusters, -1)
			}()
		}
		wg.Wait()

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
	ClustersInParallel int
	FetchPerCluster    int
	RemoveLowestPct    float64
	RerunInterval      time.Duration
	Clusters           []Cluster
	WriteToFile        bool
	ClickhouseEnabled  bool
	ClickhouseHost     string
	Listen             string
	CacheSize          uint64
	CacheTimeoutSeconds int32

	queryCache         expireCache
}{
	ClustersInParallel: 2,
	FetchPerCluster:    4,
	RerunInterval:      10 * time.Minute,
	WriteToFile:        false,
	ClickhouseEnabled:  true,
	ClickhouseHost:     "tcp://127.0.0.1:9000?debug=false",
	Listen:             "[::]:8088",
	CacheSize:          0,
	CacheTimeoutSeconds: 60,
}

func reconstructTree(data map[uint64]clickhouseField, root *flameGraphNode, minValue uint64) {
	for _, i := range root.childrenIds {
		if data[i].Value > minValue {
			node := &flameGraphNode{
				id:          data[i].Id,
				cluster:     data[i].Cluster,
				Name:        data[i].Name,
				Value:       data[i].Value,
				Total:       data[i].Total,
				parent:      root,
				childrenIds: data[i].ChildrenIds,
			}
			reconstructTree(data, node, minValue)
			root.Children = append(root.Children, node)
		}
	}
}

// Handler for the request /clusters
func clustersHandler(w http.ResponseWriter, req *http.Request) {
	t0 := time.Now()
	logger := logger.With(zap.String("handler", "clusters"))

	cacheKey := "clusters"

	if response, ok := config.queryCache.get(cacheKey); ok {
		logger.Info("request served",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusOK),
		)
		w.Write(response)
		return
	}

	connect, err := sql.Open("clickhouse", config.ClickhouseHost)
	if err != nil {
		logger.Error("error connecting to clickhouse",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
		)
		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logger.Error("exception while pinging clickhouse",
				zap.Duration("runtime", time.Since(t0)),
				zap.Int("http_code", http.StatusInternalServerError),
				zap.Int32("code", exception.Code),
				zap.String("message", exception.Message),
				zap.Any("stacktrace", exception.StackTrace),
			)
		} else {
			logger.Error("error pinging clickhouse",
				zap.Duration("runtime", time.Since(t0)),
				zap.Int("http_code", http.StatusInternalServerError),
				zap.Error(err),
			)
		}

		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}

	defer connect.Close()

	idQuery := strconv.FormatUint(rootElementId, 10)

	query := "select groupUniqArray(cluster) from flamegraph where id = " + idQuery

	var resp []string
	rows, err := connect.Query(query)
	if err != nil {
		logger.Error("Error during database query",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
		)
		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}
	for rows.Next() {
		var v []string
		err = rows.Scan(&v)
		if err != nil {
			logger.Error("Error retreiving clusters",
				zap.Duration("runtime", time.Since(t0)),
				zap.Int("http_code", http.StatusInternalServerError),
				zap.Error(err),
			)
			http.Error(w, "Error fetching data",
				http.StatusInternalServerError)
			return
		}
		resp = append(resp, v...)
	}

	b, err := json.Marshal(resp)
	if err != nil {
		logger.Error("Error marshaling data",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
		)
		http.Error(w, "Error marshaling data",
			http.StatusInternalServerError)
		return
	}
	config.queryCache.set(cacheKey, b, int32(config.RerunInterval.Seconds()))
	w.Write(b)

	logger.Info("request served",
		zap.Duration("runtime", time.Since(t0)),
		zap.Int("http_code", http.StatusOK),
	)
}

// Handler for the request /get?cluster=cluster
func timeHandler(w http.ResponseWriter, req *http.Request) {
	t0 := time.Now()
	logger := logger.With(zap.String("handler", "time"))
	// TODO: Add validation
	cluster := req.FormValue("cluster")
	if cluster == "" {
		logger.Error("You must specify cluster and ts",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(w, "Error fetching data",
			http.StatusBadRequest)
		return
	}

	cacheKey := "time&" + "&" + cluster

	logger = logger.With(
		zap.String("cluster", cluster),
	)

	if response, ok := config.queryCache.get(cacheKey); ok {
		logger.Info("request served",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusOK),
		)
		w.Write(response)
		return
	}

	lastStr := req.FormValue("last")
	last := false
	var err error
	if lastStr != "" {
		last, err = strconv.ParseBool(lastStr)
		if err != nil {
			logger.Error("Last must be true or false",
				zap.String("value", lastStr),
				zap.Duration("runtime", time.Since(t0)),
				zap.Int("http_code", http.StatusBadRequest),
			)
			http.Error(w, "Error fetching data",
				http.StatusBadRequest)
			return
		}
	}

	connect, err := sql.Open("clickhouse", config.ClickhouseHost)
	if err != nil {
		logger.Error("error connecting to clickhouse",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
		)
		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logger.Error("exception while pinging clickhouse",
				zap.Duration("runtime", time.Since(t0)),
				zap.Int("http_code", http.StatusInternalServerError),
				zap.Int32("code", exception.Code),
				zap.String("message", exception.Message),
				zap.Any("stacktrace", exception.StackTrace),
			)
		} else {
			logger.Error("error pinging clickhouse",
				zap.Duration("runtime", time.Since(t0)),
				zap.Int("http_code", http.StatusInternalServerError),
				zap.Error(err),
			)
		}

		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}

	defer connect.Close()

	idQuery := strconv.FormatUint(rootElementId, 10)

	query := "select timestamp from flamegraph where id = " + idQuery + " and cluster='" + cluster + "' order by timestamp"
	if last {
		query = "select max(timestamp) from flamegraph where id = " + idQuery + " and cluster='" + cluster + "' group by id"
	}

	var resp []int64
	rows, err := connect.Query(query)
	if err != nil {
		logger.Error("Error during database query",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
		)
		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}
	for rows.Next() {
		var v int64
		err = rows.Scan(&v)
		if err != nil {
			logger.Error("Error retreiving timestamps",
				zap.Duration("runtime", time.Since(t0)),
				zap.Int("http_code", http.StatusInternalServerError),
				zap.Error(err),
			)
			http.Error(w, "Error fetching data",
				http.StatusInternalServerError)
			return
		}
		resp = append(resp, v)
	}

	b, err := json.Marshal(struct{
		Cluster string
		Last bool
		Timestamps []int64
	}{
		Cluster: cluster,
		Last: last,
		Timestamps: resp,
	})
	if err != nil {
		logger.Error("Error marshaling data",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
		)
		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}
	config.queryCache.set(cacheKey, b, int32(config.RerunInterval.Seconds()))
	w.Write(b)

	logger.Info("request served",
		zap.Duration("runtime", time.Since(t0)),
		zap.Int("http_code", http.StatusOK),
	)
}

// Handler for the request /get?cluster=cluster&ts=timestamp
func getHandler(w http.ResponseWriter, req *http.Request) {
	t0 := time.Now()
	logger := logger.With(zap.String("handler", "get"))
	// TODO: Add validation
	ts := req.FormValue("ts")
	cluster := req.FormValue("cluster")
	if ts == "" || cluster == "" {
		logger.Error("You must specify cluster and ts",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(w, "Error fetching data",
			http.StatusBadRequest)
		return
	}

	cacheKey := "get&" + ts + "&" + cluster

	logger = logger.With(
		zap.String("cluster", cluster),
		zap.String("timestamp", ts),
	)

	if response, ok := config.queryCache.get(cacheKey); ok {
		logger.Info("request served",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusOK),
		)
		w.Write(response)
		return
	}

	connect, err := sql.Open("clickhouse", config.ClickhouseHost)
	if err != nil {
		logger.Error("error connecting to clickhouse",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
		)
		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logger.Error("exception while pinging clickhouse",
				zap.Duration("runtime", time.Since(t0)),
				zap.Int("http_code", http.StatusInternalServerError),
				zap.Int32("code", exception.Code),
				zap.String("message", exception.Message),
				zap.Any("stacktrace", exception.StackTrace),
			)
		} else {
			logger.Error("error pinging clickhouse",
				zap.Duration("runtime", time.Since(t0)),
				zap.Int("http_code", http.StatusInternalServerError),
				zap.Error(err),
			)
		}

		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}

	defer connect.Close()

	idQuery := strconv.FormatUint(rootElementId, 10)

	tsInt, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		logger.Error("Error parsing ts",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(w, "Error fetching data",
			http.StatusBadRequest)
		return
	}
	t := time.Unix(tsInt, 0)
	date := t.Format("2006-01-02")

	where := " timestamp=" + ts + " AND cluster='" + cluster + "' AND date='" + date + "'"

	rows, err := connect.Query("SELECT total FROM flamegraph WHERE" + where + " AND id = " + idQuery)
	if err != nil {
		logger.Error("Error during database query",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
		)
		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}
	total := uint64(0)
	for rows.Next() {
		err = rows.Scan(&total)
		if err != nil {
			logger.Error("Error getting total",
				zap.Duration("runtime", time.Since(t0)),
				zap.Int("http_code", http.StatusInternalServerError),
				zap.Error(err),
			)
			http.Error(w, "Error fetching data",
				http.StatusInternalServerError)
			return
		}
	}

	minValue := uint64(float64(total) * removeLowest)
	minValueQuery := strconv.FormatUint(minValue, 10)

	rows, err = connect.Query("SELECT timestamp, graph_type, cluster, id, name, total, value, children_ids FROM flamegraph WHERE" + where + " AND value > " + minValueQuery)
	if err != nil {
		logger.Error("Error during database query",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
		)
		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}
	if err != nil {
		logger.Error("Error getting data",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
		)
		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}

	data := make(map[uint64]clickhouseField)
	for rows.Next() {
		var res clickhouseField
		err := rows.Scan(&res.Timestamp, &res.GraphType, &res.Cluster, &res.Id, &res.Name, &res.Total, &res.Value, &res.ChildrenIds)
		if err != nil {
			logger.Error("Error getting data",
				zap.Duration("runtime", time.Since(t0)),
				zap.Int("http_code", http.StatusInternalServerError),
				zap.Error(err),
			)
			http.Error(w, "Error fetching data",
				http.StatusInternalServerError)
			return
		}
		data[res.Id] = res
	}

	flameGraphTreeRoot := &flameGraphNode{
		id:          data[rootElementId].Id,
		cluster:     data[rootElementId].Cluster,
		Name:        data[rootElementId].Name,
		Value:       data[rootElementId].Value,
		Total:       data[rootElementId].Total,
		parent:      nil,
		childrenIds: data[rootElementId].ChildrenIds,
	}

	reconstructTree(data, flameGraphTreeRoot, minValue)

	b, err := json.Marshal(flameGraphTreeRoot)
	if err != nil {
		logger.Error("Error marshaling data",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
		)
		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}

	config.queryCache.set(cacheKey, b, config.CacheTimeoutSeconds)
	w.Write(b)

	logger.Info("request served",
		zap.Duration("runtime", time.Since(t0)),
		zap.Int("http_code", http.StatusOK),
	)
}

func main() {
	// var flameGraph flameGraphNode
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		fmt.Printf("Error creating logger: %+v\n", err)
		os.Exit(1)
	}

	configRaw, err := ioutil.ReadFile("config.yaml")
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

	if !config.ClickhouseEnabled && !config.WriteToFile {
		logger.Fatal("Neither clickhouse no file writer enabled")
	}

	config.queryCache = expireCache{ec: ecache.New(config.CacheSize)}
	go config.queryCache.ec.ApproximateCleaner(10 * time.Second)

	logger.Info("Started",
		zap.Int("clusters", len(config.Clusters)),
		zap.Any("config", config),
	)

	removeLowest = config.RemoveLowestPct / 100

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

	mux := http.NewServeMux()
	mux.HandleFunc("/get", getHandler)
	mux.HandleFunc("/get/", getHandler)
	mux.HandleFunc("/time", timeHandler)
	mux.HandleFunc("/time/", timeHandler)
	mux.HandleFunc("/clusters", clustersHandler)
	mux.HandleFunc("/clusters/", clustersHandler)

	go processData(removeLowest)

	srv := &http.Server{
		Handler: mux,
	}

	srv.Serve(tcpListener)
}
