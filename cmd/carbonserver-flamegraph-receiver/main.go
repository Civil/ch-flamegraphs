package main

import (
	"context"
	"database/sql"
	"expvar"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	_ "net/http/pprof"
	"runtime"
	"time"

	fgpb "github.com/Civil/carbonserver-flamegraphs/flamegraphpb"
	"github.com/Civil/carbonserver-flamegraphs/helper"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/kshvakov/clickhouse"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"gopkg.in/yaml.v2"
)

var defaultLoggerConfig = zapwriter.Config{
	Logger:           "",
	File:             "stdout",
	Level:            "debug",
	Encoding:         "json",
	EncodingTime:     "iso8601",
	EncodingDuration: "seconds",
}

var errTimeout = fmt.Errorf("timeout exceeded")
var errMaxTries = fmt.Errorf("max maxTries exceeded")
var errUnknown = fmt.Errorf("unknown error")

type carbonserverCollector struct {
	endpoint     string
	root         *fgpb.FlameGraphNode
	maxTries     int
	fetchTimeout time.Duration

	client  fgpb.FlamegraphV1Client
	cleanup func()

	db *sql.DB

	metricStatChan     chan *fgpb.MultiMetricStats
	flamegraphChan     chan *fgpb.FlameGraph
	flamegraphFlatChan chan *fgpb.FlameGraphFlat
	logger             *zap.Logger
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
	accessLogger := zapwriter.Logger("access").With(
		zap.String("handler", "SendFlamegraph"),
	)
	logger := c.logger.With(zap.String("handler", "SendFlamegraph"))
	logger.Debug("data received",
		zap.Time("current_time", time.Now()),
		zap.Int("size", in.Size()),
	)

	c.flamegraphChan <- in

	accessLogger.Info("request served",
		zap.Duration("runtime", time.Since(t0)),
	)
	return &emptyResponse, nil
}

func (c carbonserverCollector) SendFlatFlamegraph(stream fgpb.FlamegraphV1_SendFlatFlamegraphServer) error {
	t0 := time.Now()
	accessLogger := zapwriter.Logger("access").With(
		zap.String("handler", "SendFlatFlamegraph"),
	)

	logger := c.logger.With(zap.String("handler", "SendFlatFlamegraph"))
	logger.Debug("data received",
		zap.Time("current_time", time.Now()),
	)

	exitChan := make(chan struct{})
	defer close(exitChan)
	dataChan := make(chan *fgpb.FlameGraphFlat, 1024)
	defer close(dataChan)

	go c.flatFlamegraphSender(dataChan, exitChan)

	packages := 0
	status := false
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			status = true
			break
		}
		if err != nil {
			logger.Error("error receiving stream",
				zap.Error(err),
			)
			break
		}
		packages++
		dataChan <- in
	}

	accessLogger.Info("request served",
		zap.Bool("status", status),
		zap.Int("total_packages_received", packages),
		zap.Duration("runtime", time.Since(t0)),
	)
	return nil
}

func (c carbonserverCollector) SendMetricsStats(stream fgpb.FlamegraphV1_SendMetricsStatsServer) error {
	t0 := time.Now()
	accessLogger := zapwriter.Logger("access").With(
		zap.String("handler", "SendMetricsStats"),
	)
	logger := zapwriter.Logger("sendMetricsStats")
	logger.Debug("data received",
		zap.Time("current_time", time.Now()),
	)

	exitChan := make(chan struct{})
	defer close(exitChan)
	dataChan := make(chan *fgpb.FlatMetricInfo, 1024)
	defer close(dataChan)

	go c.metricStatsSender(dataChan, exitChan)

	packages := 0
	status := false
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			status = true
			break
		}
		if err != nil {
			logger.Error("error receiving stream",
				zap.Error(err),
			)
			break
		}
		packages++
		dataChan <- in
	}

	accessLogger.Info("request served",
		zap.Bool("status", status),
		zap.Int("total_packages_received", packages),
		zap.Duration("runtime", time.Since(t0)),
	)
	return nil
}

func (c *carbonserverCollector) metricStatsSender(dataChan chan *fgpb.FlatMetricInfo, exitChan chan struct{}) {
	logger := zapwriter.Logger("sender").With(zap.String("type", "flatFlamegraphSender"))
	flatSender, err := helper.NewClickhouseSender(c.db, helper.MetricStatInsertQuery, time.Now().Unix(), config.Clickhouse.RowsPerInsert)
	if err != nil {
		logger.Fatal("error initializing clickhouse sender",
			zap.Error(err),
		)
	}

	cluster := ""
	timestamp := int64(0)
	packages := 0
	for {
		select {
		case <-exitChan:
			flatSender.Commit()
			logger.Debug("committed data",
				zap.Int("packages", packages),
			)
			err = flatSender.SendTimestamp("graphite_metrics", cluster, timestamp)
			if err != nil {
				logger.Error("failed update timestamp",
					zap.Error(err),
				)
			}
			return
		case ms := <-dataChan:
			if ms == nil {
				logger.Warn("received nil metricstat!")
				continue
			}
			packages++
			if cluster == "" {
				cluster = ms.Cluster
				timestamp = ms.Timestamp
			}

			err := flatSender.SendFlatMetricStatsPB(ms)
			if err != nil {
				logger.Error("failed to receive metricstats",
					zap.Error(err),
					zap.Int("packages", packages),
				)
			}
		}
	}
}

func (c *carbonserverCollector) flatFlamegraphSender(dataChan chan *fgpb.FlameGraphFlat, exitChan chan struct{}) {
	logger := zapwriter.Logger("sender").With(zap.String("type", "flatFlamegraphSender"))
	flatSender, err := helper.NewClickhouseSender(c.db, helper.FlamegraphInsertQuery, time.Now().Unix(), config.Clickhouse.RowsPerInsert)
	if err != nil {
		logger.Fatal("error initializing clickhouse sender",
			zap.Error(err),
		)
	}

	packages := 0
	cluster := ""
	timestamp := int64(0)
	for {
		select {
		case <-exitChan:
			flatSender.Commit()
			logger.Debug("committed data",
				zap.Int("packages", packages),
			)
			err = flatSender.SendTimestamp("graphite_flamegraph", cluster, timestamp)
			if err != nil {
				logger.Error("failed update timestamp",
					zap.Error(err),
				)
			}
			return
		case fg := <-dataChan:
			if fg == nil {
				logger.Warn("received nil flamegraph package!",
					zap.Int("packages", packages),
				)
				continue
			}
			packages++
			if cluster == "" {
				cluster = fg.Cluster
				timestamp = fg.Timestamp
			}
			err := flatSender.SendFlatFgPB(fg)

			if err != nil {
				logger.Error("failed to receive flamegraph",
					zap.Int("packages", packages),
					zap.Error(err),
				)
			}
		}
	}
}

func (c *carbonserverCollector) sender() {
	logger := zapwriter.Logger("sender").With(zap.String("type", "metricstat"))

	sender, err := helper.NewClickhouseSender(c.db, helper.MetricStatInsertQuery, time.Now().Unix(), config.Clickhouse.RowsPerInsert)
	if err != nil {
		logger.Fatal("error initializing clickhouse sender",
			zap.Error(err),
		)
	}

	for {
		select {
		case ms := <-c.metricStatChan:
			err := sender.SendMetricStatsPB(ms)
			if err != nil {
				logger.Error("failed to send metricstats",
					zap.Error(err),
				)
			}
		case fg := <-c.flamegraphChan:
			err := sender.SendFgPB(fg)
			if err != nil {
				logger.Error("failed to send flamegraph",
					zap.Error(err),
				)
			}
		}
	}
}

/*
virtualenv ./graphite-web -p '/usr/bin/python2.7'
source graphite-web/bin/activate
cd graphite-web/
pip install graphite-web
mv ./lib/python2.7/site-packages/opt/graphite/webapp/graphite/local_settings.py{.example,}
vim ./lib/python2.7/site-packages/opt/graphite/webapp/graphite/local_settings.py
mkdir -p ./lib/python2.7/site-packages/opt/graphite/storage/log/webapp
PYTHONPATH=./lib/python2.7/site-packages/opt/graphite/webapp/ django-admin.py migrate --settings=graphite.settings --run-syncdb
*/

func newCarbonserverCollector(db *sql.DB) (*carbonserverCollector, error) {
	collector := carbonserverCollector{
		db:                 db,
		metricStatChan:     make(chan *fgpb.MultiMetricStats, 1024),
		flamegraphChan:     make(chan *fgpb.FlameGraph, 1024),
		flamegraphFlatChan: make(chan *fgpb.FlameGraphFlat, 1024),
		logger:             zapwriter.Logger("main"),
	}

	go collector.sender()

	return &collector, nil
}

type connectOptions struct {
	Insecure bool `yaml:"insecure"`
}

var config = struct {
	Clickhouse         helper.ClickhouseConfig `yaml:"clickhouse"`
	Listen             string                  `yaml:"listen"`
	ConnectOptions     connectOptions          `yaml:"connect_options"`
	CacheSize          uint64
	MaxSendMessageSize uint32 `yaml:"max_send_message_size"`
	MaxRecvMessageSize uint32 `yaml:"max_receive_message_size"`
	DumpInterval       time.Duration

	Logger []zapwriter.Config `yaml:"logger"`

	db *sql.DB
}{
	Clickhouse: helper.ClickhouseConfig{
		ClickhouseHost:         "tcp://127.0.0.1:9000?debug=false",
		RowsPerInsert:          1000000,
		UseDistributedTables:   false,
		DistributedClusterName: "flamegraph",
	},
	Listen: "[::]:8088",
	ConnectOptions: connectOptions{
		Insecure: true,
	},
	CacheSize:          10000,
	MaxSendMessageSize: 1.5 * 1024 * 1024 * 1024,
	MaxRecvMessageSize: 1.5 * 1024 * 1024 * 1024,
	DumpInterval:       120 * time.Second,

	Logger: []zapwriter.Config{defaultLoggerConfig},
}

func validateConfig() {
	logger := zapwriter.Logger("main")
	switch {
	case config.Listen == "":
		logger.Fatal("listen can't be empty")
	case config.Clickhouse.ClickhouseHost == "":
		logger.Fatal("clickhouse host can't be empty")
	}
}

// BuildVersion is defined at build and reported at startup and as expvar
var BuildVersion = "(development version)"

func main() {
	// var flameGraph flameGraphNode
	err := zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})
	if err != nil {
		log.Fatal("failed to initialize logger with default configuration")

	}
	logger := zapwriter.Logger("main")

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
			zap.Any("config", config),
		)
	}
	// Reinitialize logger
	logger = zapwriter.Logger("main")

	// Initialize DB Connection
	db, err := sql.Open("clickhouse", config.Clickhouse.ClickhouseHost)
	if err != nil {
		logger.Fatal("error connecting to clickhouse",
			zap.Error(err),
			zap.Any("config", config),
		)
	}

	if err = db.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logger.Fatal("exception while pinging clickhouse",
				zap.Int32("code", exception.Code),
				zap.String("message", exception.Message),
				zap.Any("stacktrace", exception.StackTrace),
				zap.Any("config", config),
			)
		}
		logger.Fatal("error pinging clickhouse",
			zap.Error(err),
			zap.Any("config", config),
		)
	}

	migrateOrCreateTables(db)

	// Initialize Collector
	collector, err := newCarbonserverCollector(db)
	if err != nil {
		logger.Fatal("failed to initialize collector",
			zap.Error(err),
			zap.Any("config", config),
		)
	}

	// Initialize gRPC Server
	tcpAddr, err := net.ResolveTCPAddr("tcp", config.Listen)
	if err != nil {
		logger.Fatal("error resolving address",
			zap.Error(err),
			zap.Any("config", config),
		)
	}
	tcpListener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		logger.Fatal("error binding to address",
			zap.Error(err),
			zap.Any("config", config),
		)
	}

	grpcServer := grpc.NewServer(
		grpc.RPCDecompressor(grpc.NewGZIPDecompressor()),
		grpc.RPCCompressor(grpc.NewGZIPCompressor()),
		grpc.MaxRecvMsgSize(int(config.MaxRecvMessageSize)),
		grpc.MaxSendMsgSize(int(config.MaxSendMessageSize)),
	)
	fgpb.RegisterFlamegraphV1Server(grpcServer, collector)

	logger.Info("started",
		zap.Any("config", config),
	)

	expvar.NewString("GoVersion").Set(runtime.Version())
	expvar.NewString("BuildVersion").Set(BuildVersion)

	err = grpcServer.Serve(tcpListener)
	if err != nil {
		logger.Fatal("unexpected error from grpc server",
			zap.Error(err),
			zap.Any("config", config),
		)
	}
}
