package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"database/sql"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"github.com/Civil/carbonserver-flamegraphs/helper"
	"github.com/Civil/carbonserver-flamegraphs/types"

	"github.com/NYTimes/gziphandler"
	"github.com/kshvakov/clickhouse"
)

var defaultLoggerConfig = zapwriter.Config{
	Logger:           "",
	File:             "stderr",
	Level:            "info",
	Encoding:         "console",
	EncodingTime:     "iso8601",
	EncodingDuration: "seconds",
}

func clickhouseSender(sender *helper.ClickhouseSender, node *types.StackFlameGraphNode, now int64) error {
	err := sender.SendStacktrace(node)
	if err != nil {
		return err
	}

	for _, c := range node.Children {
		err = clickhouseSender(sender, c, now)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
	Id           int64 `json:"-"`
	Application  string `json:"-"`
	Instance     string `json:"-"`
	FunctionName string `json:"name"`
	FileName     string `json:"file"`
	Line         int64 `json:"line"`
	Samples      int64 `json:"samples"`
	MaxSamples   int64         `json:"maxSamples"`
	Children    []*StackFlameGraphNode `json:"children,omitempty"`
	ChildrenIds []int64 `json:"-"`
	Parent      *StackFlameGraphNode `json:"-"`
*/

func writer(data <-chan []byte, exit <-chan struct{}) {
	var v types.StackFlameGraphNode
	logger := config.logger.With(zap.String("function", "writer"))
	for {
		select {
		case <-exit:
			return
		case d := <-data:
			err := json.Unmarshal(d, &v)
			if err != nil {
				logger.Error("failed to parse json",
					zap.String("data", string(d)),
					zap.Error(err),
				)
				continue
			}
			now := time.Now().Unix()
			sender, err := helper.NewClickhouseSender(
				config.db,
				"INSERT INTO stacktrace (Timestamp, ID, Application, Instance, FunctionName, FileName, Line, Samples, MaxSamples, ChildrenIDs, ParentID, Date, Version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
				now,
				1000000)
			if err != nil {
				logger.Error("failed to initialize DB sender",
					zap.Error(err),
				)
				continue
			}
			err = clickhouseSender(sender, &v, now)
			if err != nil {
				logger.Error("failed to send data in database",
					zap.Error(err),
				)
				continue
			}
			sender.Commit()
			sender, err = helper.NewClickhouseSender(
				config.db,
				"INSERT INTO stacktraceTimestamps (Timestamp, Application, Instance, Date, Version) VALUES (?, ?, ?, ?, ?)",
				now,
				1000000)
			if err != nil {
				logger.Error("failed to send timestamp in database",
					zap.Error(err),
				)
				continue
			}
			err = sender.SendStacktraceTimestamp(&v)
			if err != nil {
				logger.Error("failed to send timestamp in database",
					zap.Error(err),
				)
				continue
			}
			sender.Commit()
		}
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	logger := config.logger.With(zap.String("function", "handler"))
	logger.Info("Received request",
		zap.String("url", r.URL.String()),
	)
	var err error
	var reader io.ReadCloser
	switch r.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(r.Body)
		if err != nil {
			logger.Error("failed to process request",
				zap.Error(err),
			)
			return
		}
		defer reader.Close()
	default:
		reader = r.Body
	}

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		logger.Error("failed to read request body",
			zap.Error(err),
		)
		return
	}

	config.writeChan <- body
}

var config = struct {
	helper.ClickhouseConfig

	Listen string

	logger    *zap.Logger
	db        *sql.DB
	writeChan chan []byte
	exitChan  chan struct{}
}{
	ClickhouseConfig: helper.ClickhouseConfig{
		ClickhouseHost: "tcp://127.0.0.1:9000?debug=false",
		RowsPerInsert:  100000,

		UseDistributedTables:   false,
		DistributedClusterName: "",
	},

	Listen: ":8080",
}

// Create table functions
const (
	schema_version uint64 = 1
)

func createStackTraceTable(tablePostfix, engine string) error {
	_, err := config.db.Exec("CREATE TABLE IF NOT EXISTS stacktrace" + tablePostfix + ` (
			Timestamp Int64,
			ID Int64,
			Application String,
			Instance String,
			FunctionName String,
			FileName String,
			Line Int64,
			Samples Int64,
			MaxSamples Int64,
			ChildrenIDs Array(Int64),
			ParentID UInt64,
			Date Date,
			Version UInt64 DEFAULT 0
		) engine=` + engine)

	return err
}

func createStackTraceTimestampsTable(tablePostfix, engine string) error {
	_, err := config.db.Exec("CREATE TABLE IF NOT EXISTS stacktraceTimestamps" + tablePostfix + ` (
			Timestamp Int64,
			Application String,
			Instance String,
			Date Date,
			Version UInt64 DEFAULT 0
		) engine=` + engine)

	return err
}

func createLocalTables(tablePostfix string) error {
	_, err := config.db.Exec(`
		CREATE TABLE IF NOT EXISTS stacktrace_table_version_local (
			SchemaVersion Int64,
			Date Date,
			Version UInt64
		) engine=ReplacingMergeTree(Date, (SchemaVersion, Date), 8192, Version)
	`)

	if err != nil {
		return err
	}

	err = createStackTraceTable(tablePostfix, "MergeTree(Date, (Timestamp, Application, Instance, FunctionName, FileName, Line, Samples, ChildrenIDs, ParentID), 8192)")
	if err != nil {
		return err
	}
	err = createStackTraceTimestampsTable(tablePostfix, "MergeTree(Date, (Timestamp, Application, Instance), 8192)")
	if err != nil {
		return err
	}
	return err
}

func createDistributedTables() error {
	err := createStackTraceTimestampsTable("", "Distributed(stacktraceTimestamps, 'default', 'stacktraceTimestamps_local', sipHash64(Application))")
	if err != nil {
		return err
	}
	err = createStackTraceTable("", "Distributed(stacktrace, 'default', 'stacktrace_local', sipHash64(FunctionName))")
	return err
}

func migrateOrCreateTables() {
	logger := config.logger.With(zap.String("function", "migrateOrCreateTables"))
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

	rows, err := config.db.Query("SELECT max(SchemaVersion) FROM stacktrace_table_version_local")
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

		stmt, err := tx.Prepare("INSERT INTO stacktrace_table_version_local (SchemaVersion, Date, Version) VALUES (?, ?, ?)")
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
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Printf("Error creating logger: %+v\n", err)
		os.Exit(1)
	}

	cfgPath := flag.String("config", "config.yaml", "path to the config file")
	flag.Parse()

	configRaw, err := ioutil.ReadFile(*cfgPath)
	if err != nil {
		logger.Error("Error reading configfile 'config.yaml'",
			zap.Error(err),
		)
	}

	if configRaw != nil {
		err = yaml.Unmarshal(configRaw, &config)
		if err != nil {
			logger.Fatal("Error parsing config file",
				zap.Error(err),
			)
		}
	}

	err = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})
	if err != nil {
		logger.Fatal("error applying logger config",
			zap.Error(err),
		)
	}
	config.logger = zapwriter.Logger("StackTraceReceiver")

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

	config.writeChan = make(chan []byte)
	config.exitChan = make(chan struct{})

	go writer(config.writeChan, config.exitChan)

	h := http.HandlerFunc(handler)

	z := gziphandler.GzipHandler(h)

	http.Handle("/", z)
	config.logger.Info("Started")
	http.ListenAndServe(config.Listen, nil)
}
