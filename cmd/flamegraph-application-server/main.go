package main

import (
	"encoding/json"
	"flag"
	"fmt"

	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"database/sql"

	"strconv"

	ecache "github.com/dgryski/go-expirecache"
	"github.com/kshvakov/clickhouse"

	"github.com/Civil/ch-flamegraphs/helper"
)

var logger *zap.Logger

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
	RemoveLowestPct     float64
	ClickhouseHost      string
	Listen              string
	CacheSize           uint64
	CacheTimeoutSeconds int32
	RerunInterval       time.Duration

	queryCache expireCache
	db         *sql.DB
}{
	ClickhouseHost:      "tcp://127.0.0.1:9000?debug=false",
	Listen:              "[::]:8088",
	CacheSize:           0,
	CacheTimeoutSeconds: 60,
	RerunInterval:       10 * time.Minute,
}

// v1 API

// Handler for the request /get?cluster=cluster
func timeStackHandler(w http.ResponseWriter, req *http.Request) {
	t0 := time.Now()
	logger := logger.With(zap.String("handler", "time"))
	// TODO: Add validation
	application := req.FormValue("application")
	if application == "" {
		logger.Error("You must specify application",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(w, "Error fetching data",
			http.StatusBadRequest)
		return
	}

	cacheKey := "time&" + "&" + application

	logger = logger.With(
		zap.String("application", application),
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

	if err := config.db.Ping(); err != nil {
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

	query := "select Timestamp from stacktraceTimestamps where Application='" + application + "' order by Timestamp"
	if last {
		query = "select max(Timestamp) from stacktraceTimestamps where Application='" + application + "' order by Timestamp"
	}

	var resp []int64
	rows, err := config.db.Query(query)
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

	resp = append(resp, int64(0))

	b, err := json.Marshal(struct {
		Application string
		Last        bool
		Timestamps  []int64
	}{
		Application: application,
		Last:        last,
		Timestamps:  resp,
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

func applicationStackHandler(w http.ResponseWriter, req *http.Request) {
	t0 := time.Now()
	logger := logger.With(zap.String("handler", "time"))

	if err := config.db.Ping(); err != nil {
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

	query := "select distinct Application from stacktraceTimestamps order by Application"

	var resp []string
	rows, err := config.db.Query(query)
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
		var v string
		err = rows.Scan(&v)
		if err != nil {
			logger.Error("Error retreiving applications",
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

	b, err := json.Marshal(struct {
		Applications []string
	}{
		Applications: resp,
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
	w.Write(b)

	logger.Info("request served",
		zap.Duration("runtime", time.Since(t0)),
		zap.Int("http_code", http.StatusOK),
	)
}

// Handler for the request /get?cluster=cluster&ts=timestamp
func getStackHandler(w http.ResponseWriter, req *http.Request) {
	var err error
	t0 := time.Now()
	logger := logger.With(zap.String("handler", "get"))
	// TODO: Add validation
	ts := req.FormValue("ts")
	application := req.FormValue("application")
	instance := req.FormValue("instance")
	if ts == "" || application == "" {
		logger.Error("You must specify cluster and ts",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(w, "Error parsing 'ts' or 'cluster'", http.StatusBadRequest)
		return
	}
	samples := req.FormValue("samples")

	showFileNames := false
	showFileNamesStr := req.FormValue("show_files")
	if showFileNamesStr != "" {
		showFileNames, _ = strconv.ParseBool(showFileNamesStr)
	}

	cacheKey := ""
	if ts != "0" {
		cacheKey = "get&" + ts + "&" + application + "&" + instance
	}

	logger = logger.With(
		zap.String("application", application),
		zap.String("instance", instance),
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

	query, err := helper.NewQuery(config.db, ts, application, instance, samples, 0)
	if err != nil {
		http.Error(w, "Error fetching data",
			http.StatusBadRequest)
		return
	}
	flameGraphTreeRoot, err := query.GetStackFlamegraph(showFileNames)
	if err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logger.Error("exception while getting data",
				zap.Duration("runtime", time.Since(t0)),
				zap.Int("http_code", http.StatusInternalServerError),
				zap.Int32("code", exception.Code),
				zap.String("message", exception.Message),
				zap.Any("stacktrace", exception.StackTrace),
			)
		} else {
			logger.Error("error getting data",
				zap.Duration("runtime", time.Since(t0)),
				zap.Int("http_code", http.StatusInternalServerError),
				zap.Error(err),
			)
		}

		http.Error(w, "Error fetching data",
			http.StatusInternalServerError)
		return
	}

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

	if ts != "0" {
		config.queryCache.set(cacheKey, b, config.CacheTimeoutSeconds)
	}
	w.Write(b)

	logger.Info("request served",
		zap.Duration("runtime", time.Since(t0)),
		zap.Int("http_code", http.StatusOK),
	)
}

func cors(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		fn(w, r)
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

	mux := http.NewServeMux()

	mux.HandleFunc("/v1/stack/get/", cors(getStackHandler))
	mux.HandleFunc("/v1/stack/time/", cors(timeStackHandler))
	mux.HandleFunc("/v1/stack/applications/", cors(applicationStackHandler))

	srv := &http.Server{
		Handler: mux,
	}

	logger.Info("Started",
		zap.Any("config", config),
	)

	srv.Serve(tcpListener)
}
