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

	sender, err := helper.NewClickhouseSender(c.db, helper.FlamegraphInsertQuery, time.Now().Unix(), config.Clickhouse.RowsPerInsert)
	if err != nil {
		state.Reason = "failed to initialize clickhouse timestamp sender"
		state.IsError = true
		state.Error = err.Error()
		return err

	}
	err = sender.SendTimestamp("graphite_flamegraph", state.Cluster, state.Timestamp)
	if err != nil {
		state.Reason = "failed to update clickhouse timestamp sender"
		state.IsError = true
		state.Error = err.Error()
		return err
	}

	logger.Debug("all sent",
		zap.Duration("runtime", time.Since(t0)),
		zap.Int64("packages", state.PackagesReceived),
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
	fgSender, err := helper.NewClickhouseSender(c.db, helper.FlamegraphInsertQuery, time.Now().Unix(), config.Clickhouse.RowsPerInsert)
	if err != nil {
		logger.Fatal("error initializing clickhouse sender",
			zap.String("type", "flamegraph"),
			zap.Error(err),
			zap.Stack("stack"),
		)
	}

	msSender, err := helper.NewClickhouseSender(c.db, helper.MetricStatInsertQuery, time.Now().Unix(), config.Clickhouse.RowsPerInsert)
	if err != nil {
		logger.Fatal("error initializing clickhouse sender",
			zap.String("type", "metricstats"),
			zap.Error(err),
			zap.Stack("stack"),
		)
	}

	packages := 0
	for {
		select {
		case <-c.exitChan:
			_, err = fgSender.Commit()
			if err != nil {
				logger.Error("failed to commit",
					zap.String("type", "flamegraph"),
					zap.Error(err),
				)
			}
			_, err = msSender.Commit()
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
		case <-c.forceCommitChan:
			logger.Debug("doing garceful commit")
			err = fgSender.GracefulCommit()
			if err == helper.ErrGracefulCommitNotEnoughLines {
				logger.Debug("not enough lines for gracefulCommit",
					zap.String("type", "metricstats"),
				)
			} else {
				logger.Error("graceful commit failed",
					zap.Error(err),
				)
			}

			err = msSender.GracefulCommit()
			if err == helper.ErrGracefulCommitNotEnoughLines {
				logger.Debug("not enough lines for gracefulCommit",
					zap.String("type", "flamegraph"),
				)
			} else {
				logger.Error("graceful commit failed",
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
					zap.String("type", "metricstats"),
					zap.Int("packages", packages),
					zap.Error(err),
				)
				fgSender.Commit()
				fgSender, err = helper.NewClickhouseSender(c.db, helper.FlamegraphInsertQuery, time.Now().Unix(), config.Clickhouse.RowsPerInsert)
				if err != nil {
					logger.Fatal("error initializing clickhouse sender",
						zap.String("type", "flamegraph"),
						zap.Error(err),
					)
				}
			}
		case ms := <-c.msChan:
			if ms == nil {
				logger.Debug("received nil",
					zap.String("type", "metricstats"),
				)
				continue
			}
			packages++

			err := msSender.SendFlatMetricStatsPB(ms)
			if err != nil {
				logger.Error("failed to store",
					zap.String("type", "metricstats"),
					zap.Int("packages", packages),
					zap.Error(err),
				)
				msSender.Commit()
				msSender, err = helper.NewClickhouseSender(c.db, helper.MetricStatInsertQuery, time.Now().Unix(), config.Clickhouse.RowsPerInsert)
				if err != nil {
					logger.Fatal("error initializing clickhouse sender",
						zap.String("type", "metricstats"),
						zap.Error(err),
					)
				}
			}
		}
	}
}
