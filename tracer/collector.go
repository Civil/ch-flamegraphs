package tracer

/*
Copyright (c) 2016, StackImpact GmbH. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the StackImpact GmbH nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"runtime/pprof"
	"time"

	"net/http"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	"github.com/Civil/carbonserver-flamegraphs/helper"
	"github.com/Civil/carbonserver-flamegraphs/types"
	"github.com/google/pprof/profile"

	"database/sql"
	_ "github.com/kshvakov/clickhouse"
)

var defaultLoggerConfig = zapwriter.Config{
	Logger:           "",
	File:             "stdout",
	Level:            "info",
	Encoding:         "console",
	EncodingTime:     "iso8601",
	EncodingDuration: "seconds",
}

type Config struct {
	Application        string
	Instance           string
	Endpoint           string
	ProfileDuration    time.Duration
	CollectionInterval time.Duration
	Debug              bool
}

type StackTracer struct {
	Config

	logger *zap.Logger

	root *types.StackFlameGraphNode

	clickhouse helper.ClickhouseConfig
	db         *sql.DB
}

func NewStackTracer(config Config) *StackTracer {
	if config.Debug {
		defaultLoggerConfig.Level = "debug"
	}
	err := zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})
	if err != nil {
		fmt.Printf("Error applying config for logger: %+v\n", err)
		return nil
	}

	st := &StackTracer{
		Config: config,
		logger: zapwriter.Logger("StackTracer"),

		root: types.NewStackFlamegraphTree("all", config.Instance, config.Application),
	}

	return st
}

func (st *StackTracer) Start() {
	go st.startReporting()
}

// Main loop
func (st *StackTracer) startReporting() {
	logger := st.logger.With(zap.String("function", "startReporting"))
	tickerProfile := time.NewTicker(st.ProfileDuration)
	tickerSend := time.NewTicker(st.CollectionInterval)

	for {
		select {
		case <-tickerSend.C:
			err := st.send()
			if err != nil {
				logger.Error("Failed to send POST request",
					zap.Error(err),
				)
			}
		case <-tickerProfile.C:
			st.report("timer")
		}
	}
}

func (st *StackTracer) send() error {
	d, _ := json.Marshal(st.root)

	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write(d)
	w.Close()

	url := st.Endpoint + "/v1/stacktrace"
	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")

	var httpClient = &http.Client{
		Timeout: time.Second * 10,
	}
	_, err = httpClient.Do(req)
	if err != nil {
		return err
	}

	return nil
}

func (st *StackTracer) report(trigger string) {
	logger := st.logger.With(
		zap.Duration("profileDuration", st.ProfileDuration),
		zap.String("function", "report"),
	)
	logger.Debug("profile iteration started")
	p, err := st.readCPUProfile(st.ProfileDuration)
	if err != nil {
		logger.Error("failed to read cpu profile",
			zap.Error(err),
		)
		return
	}
	if p == nil {
		return
	}
	logger.Debug("profile stopped",
		zap.Any("profile", p),
	)

	if err := st.updateCPUCallGraph(p); err != nil {
		logger.Error("failed to create CPU Call Graph",
			zap.Error(err),
		)
	}
}

func (st *StackTracer) updateCPUCallGraph(p *profile.Profile) error {
	logger := st.logger.With(zap.String("function", "updateCPUCallGraph"))
	// find "samples" type index
	typeIndex := -1
	for i, s := range p.SampleType {
		if s.Type == "samples" {
			typeIndex = i

			break
		}
	}

	if typeIndex == -1 {
		return errors.New("Unrecognized profile data")
	}

	// calculate total possible samples
	var maxSamples int64
	if pt := p.PeriodType; pt != nil && pt.Type == "cpu" && pt.Unit == "nanoseconds" {
		maxSamples = (p.DurationNanos / p.Period) * int64(runtime.NumCPU())
	} else {
		return errors.New("No period information in profile")
	}

	st.root = types.NewStackFlamegraphTree("total cpu", st.root.Instance, st.root.Application)
	st.root.MaxSamples += maxSamples

	for _, s := range p.Sample {
		if len(s.Value) <= typeIndex {
			logger.Warn("Possible inconsistence in profile types and measurements",
				zap.Int("typeIndex", typeIndex),
				zap.Int("len_value", len(s.Value)),
			)
			continue
		}

		stackSamples := s.Value[typeIndex]
		st.root.Increment(stackSamples)

		currentNode := st.root
		for i := len(s.Location) - 1; i >= 0; i-- {
			l := s.Location[i]
			funcName, fileName, fileLine := readFuncInfo(l)

			if funcName == "runtime.goexit" {
				continue
			}

			currentNode = currentNode.FindOrAdd(funcName, fileName, fileLine, stackSamples)
		}
	}

	return nil
}

func (st *StackTracer) readCPUProfile(duration time.Duration) (*profile.Profile, error) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)

	start := time.Now()

	err := pprof.StartCPUProfile(w)
	if err != nil {
		return nil, err
	}

	done := make(chan bool)
	timer := time.NewTimer(duration)
	// TODO: Check if it actually should be done in that way
	go func() {
		<-timer.C

		pprof.StopCPUProfile()

		done <- true
	}()
	<-done

	w.Flush()
	r := bufio.NewReader(&buf)

	if p, perr := profile.Parse(r); perr == nil {
		if p.TimeNanos == 0 {
			p.TimeNanos = start.UnixNano()
		}
		if p.DurationNanos == 0 {
			p.DurationNanos = duration.Nanoseconds()
		}

		if serr := symbolizeProfile(p); serr != nil {
			return nil, serr
		}

		if verr := p.CheckValid(); verr != nil {
			return nil, verr
		}

		return p, nil
	} else {
		return nil, perr
	}
}

func symbolizeProfile(p *profile.Profile) error {
	functions := make(map[string]*profile.Function)

	for _, l := range p.Location {
		if l.Address != 0 && len(l.Line) == 0 {
			if f := runtime.FuncForPC(uintptr(l.Address)); f != nil {
				name := f.Name()
				fileName, lineNumber := f.FileLine(uintptr(l.Address))

				pf := functions[name]
				if pf == nil {
					pf = &profile.Function{
						ID:         uint64(len(p.Function) + 1),
						Name:       name,
						SystemName: name,
						Filename:   fileName,
					}

					functions[name] = pf
					p.Function = append(p.Function, pf)
				}

				line := profile.Line{
					Function: pf,
					Line:     int64(lineNumber),
				}

				l.Line = []profile.Line{line}
				if l.Mapping != nil {
					l.Mapping.HasFunctions = true
					l.Mapping.HasFilenames = true
					l.Mapping.HasLineNumbers = true
				}
			}
		}
	}

	return nil
}

func readFuncInfo(l *profile.Location) (funcName string, fileName string, fileLine int64) {
	for li := range l.Line {
		if fn := l.Line[li].Function; fn != nil {
			return fn.Name, fn.Filename, l.Line[li].Line
		}
	}

	return "", "", 0
}
