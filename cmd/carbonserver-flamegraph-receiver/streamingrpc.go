package main

import (
	"context"
	"io"
	"time"

	"github.com/Civil/ch-flamegraphs/fglogpb"
	fgpb "github.com/Civil/ch-flamegraphs/flamegraphpb"
	"github.com/Civil/ch-flamegraphs/helper"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

func (c carbonserverCollector) SendFlatFlamegraph(stream fgpb.FlamegraphV1_SendFlatFlamegraphServer) error {
	t0 := time.Now()
	state := &fglogpb.AccessLogger{
		Handler: "SendFlatFlamegraph",
	}

	defer func() {
		c.logState(context.Background(), t0, state)
	}()

	logger := c.logger.With(zap.String("handler", "SendFlatFlamegraph"))
	logger.Debug("data received",
		zap.Time("current_time", time.Now()),
	)

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			logger.Debug("got EOF")
			break
		}
		if err != nil {
			state.IsError = true
			state.Error = err.Error()
			state.Reason = "failed to process stream"
			return err
		}
		state.PackagesReceived++
		state.BytesReceived += int64(in.Size())
		if state.Timestamp == 0 {
			state.Timestamp = in.Timestamp
			state.Cluster = in.Cluster
			state.SourceServer = in.Server
		}
		c.fgChan <- in
	}

	logger.Debug("state",
		zap.Any("state", state),
		)

	sender := helper.NewClickhouseHTTPSender(c.chServers, helper.FlamegraphTimestampInsertHTTPQuery)
	sender.NewRequest()
	err := sender.SendTimestampHTTP("graphite_flamegraph", state.Cluster, state.Timestamp)
	if err != nil {
		state.Reason = "failed to update clickhouse timestamp sender"
		state.IsError = true
		state.Error = err.Error()
		return err
	}

	err = sender.Commit()
	if err != nil {
		state.Reason = "failed to update clickhouse timestamp sender"
		state.IsError = true
		state.Error = err.Error()
		return err
	}

	logger.Debug("all sent",
		zap.Duration("runtime", time.Since(t0)),
		zap.Int64("packages", state.PackagesReceived),
		zap.Any("state", state),
	)
	return nil
}

func (c carbonserverCollector) SendMetricsStats(stream fgpb.FlamegraphV1_SendMetricsStatsServer) error {
	t0 := time.Now()
	state := fglogpb.AccessLogger{
		Handler: "SendMetricsStats",
	}
	defer func() {
		c.logState(context.Background(), t0, &state)
	}()

	logger := zapwriter.Logger("sendMetricsStats")
	logger.Debug("data received",
		zap.Time("current_time", time.Now()),
	)

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			state.IsError = true
			state.Error = err.Error()
			state.Reason = "failed to process stream"

			return err
		}
		state.PackagesReceived++
		state.BytesReceived += int64(in.Size())
		if state.Timestamp == 0 {
			state.Timestamp = in.Timestamp
			state.Cluster = in.Cluster
			state.SourceServer = in.Server
		}
		c.msChan <- in
	}

	sender, err := helper.NewClickhouseSender(c.db, helper.MetricStatInsertQuery, time.Now().Unix(), config.Clickhouse.RowsPerInsert)
	if err != nil {
		state.IsError = true
		state.Error = err.Error()
		state.Reason = "failed to initialize clickhouse timestamp sender"

		return err

	}
	err = sender.SendTimestamp("graphite_metrics", state.Cluster, state.Timestamp)
	if err != nil {
		state.IsError = true
		state.Error = err.Error()
		state.Reason = "failed to update clickhouse timestamp sender"

		return err
	}

	logger.Debug("all sent",
		zap.Duration("runtime", time.Since(t0)),
		zap.Int64("packages", state.PackagesReceived),
	)

	return nil
}

func (c *carbonserverCollector) flatSender() {
	logger := zapwriter.Logger("flatSender")
	fgSender := helper.NewClickhouseHTTPSender(c.chServers, helper.FlamegraphInsertHttpQuery)
	fgSender.NewRequest()

	msSender := helper.NewClickhouseHTTPSender(c.chServers, helper.MetricStatInsertHTTPQuery)
	msSender.NewRequest()

	clusterSender := helper.NewClickhouseHTTPSender(c.chServers, helper.FlamegraphClusterInsertHTTPQuery)
	clusterSender.NewRequest()

	packages := 0
	ticker := time.NewTicker(1 * time.Second)
	var err error
	for {
		select {
		case <-c.exitChan:
			err = fgSender.Commit()
			if err != nil {
				logger.Error("failed to commit",
					zap.String("type", "flamegraph"),
					zap.Error(err),
				)
			}
			err = msSender.Commit()
			if err != nil {
				logger.Error("failed to commit",
					zap.String("type", "metricstats"),
					zap.Error(err),
				)
			}
			logger.Debug("committed data",
				zap.String("type", "all"),
				zap.Int("packages", packages),
			)

			return
		case <-ticker.C:
			logger.Debug("doing commit")
			err = fgSender.Commit()
			if err != nil {
				logger.Error("commit failed",
					zap.Error(err),
				)
			}

			err = msSender.Commit()
			if err != nil {
				logger.Error("commit failed",
					zap.Error(err),
				)
			}
		case fg := <-c.fgChan:
			if fg == nil {
				logger.Debug("received nil package!",
					zap.String("type", "flamegraph"),
					zap.Int("packages", packages),
				)
				continue
			}
			packages++
			err := fgSender.SendFlatFgPB(fg)

			if err != nil {
				logger.Error("failed to store",
					zap.String("type", "flamegraph"),
					zap.Int("packages", packages),
					zap.Error(err),
				)
			}

			knownClusters.Lock()
			if _, ok := knownClusters.names[fg.Cluster]; !ok {
				knownClusters.names[fg.Cluster] = struct{}{}
				err := clusterSender.SendClusterHTTP(fg.Cluster, fg.Server)
				if err != nil {
					logger.Error("failed to send cluster name",
						zap.Error(err),
					)
				}
				clusterSender.Commit()
			}
			knownClusters.Unlock()
		case ms := <-c.msChan:
			if ms == nil {
				logger.Debug("received nil",
					zap.String("type", "metricstats"),
				)
				continue
			}
			packages++

			err := msSender.SendMetricstatsHTTP(ms)
			if err != nil {
				logger.Error("failed to store",
					zap.String("type", "metricstats"),
					zap.Int("packages", packages),
					zap.Error(err),
				)
			}
		}
	}
}
