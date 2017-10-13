package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	_ "net/http/pprof"
	"time"

	fgpb "github.com/Civil/carbonserver-flamegraphs/flamegraphpb"
	ecache "github.com/dgryski/go-expirecache"
	"github.com/golang/protobuf/ptypes/empty"
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
}

var emptyResponse empty.Empty

func (c carbonserverCollector) GetVersion(ctx context.Context, empty *empty.Empty) (*fgpb.ProtocolVersionResponse, error) {
	t0 := time.Now()
	logger := zapwriter.Logger("access").With(
		zap.String("handler", "GetVersion"),
	)
	resp := &fgpb.ProtocolVersionResponse{Version: 0}
	logger.Info("request served",

		zap.Duration("runtime", time.Since(t0)),
	)
	return resp, nil

}

func (c carbonserverCollector) SendFlamegraph(ctx context.Context, in *fgpb.FlameGraphNode) (*empty.Empty, error) {
	t0 := time.Now()
	logger := zapwriter.Logger("access").With(
		zap.String("handler", "SendFlamegraph"),
	)
	logger.Debug("data received",
		zap.Time("current_time", time.Now()),
		zap.Int("size", in.Size()),
	)
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
	logger.Info("request served",
		zap.Duration("runtime", time.Since(t0)),
	)
	return &emptyResponse, nil
}

func newCarbonserverCollector() (carbonserverCollector, error) {
	collector := carbonserverCollector{}

	return collector, nil
}

type connectOptions struct {
	Insecure bool `yaml:"insecure"`
}

var config = struct {
	Listen         string         `yaml:"listen"`
	ConnectOptions connectOptions `yaml:"connect_options"`
	CacheSize      uint64

	Logger []zapwriter.Config `yaml:"logger"`

	queryCache expireCache
}{
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

	collector, err := newCarbonserverCollector()
	if err != nil {
		logger.Fatal("failed to initialize collector",
			zap.Error(err),
		)
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
