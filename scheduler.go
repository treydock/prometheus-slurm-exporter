/* Copyright 2017 Victor Penso, Matteo Dessalvi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"bytes"
	"context"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

/*
 * Execute the Slurm sdiag command to read the current statistics
 * from the Slurm scheduler. It will be repreatedly called by the
 * collector.
 */

// Basic metrics for the scheduler
type SchedulerMetrics struct {
	threads                             float64
	agent_queue_size                    float64
	agent_count                         float64
	agent_thread_count                  float64
	dbd_queue_size                      float64
	last_cycle                          float64
	mean_cycle                          float64
	mean_depth_cycle                    float64
	cycle_per_minute                    float64
	last_queue_length                   float64
	backfill_last_cycle                 float64
	backfill_mean_cycle                 float64
	backfill_last_depth_cycle           float64
	backfill_last_depth_cycle_try_sched float64
	backfill_depth_mean                 float64
	backfill_depth_mean_try_sched       float64
	backfill_depth_mean_try_depth       float64
	backfill_last_queue_length          float64
	backfill_queue_length_mean          float64
	backfill_last_table_size            float64
	backfill_mean_table_size            float64
	total_backfilled_jobs_since_start   float64
	total_backfilled_jobs_since_cycle   float64
	total_backfilled_heterogeneous      float64
	rpc_stats_count                     map[string]float64
	rpc_stats_avg_time                  map[string]float64
	rpc_stats_total_time                map[string]float64
	user_rpc_stats_count                map[string]float64
	user_rpc_stats_avg_time             map[string]float64
	user_rpc_stats_total_time           map[string]float64
}

// Execute the sdiag command and return its output
func SchedulerData(logger log.Logger) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*collectorTimeout)*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sdiag")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			level.Error(logger).Log("msg", "Timeout executing sdiag")
			return "", ctx.Err()
		} else {
			level.Error(logger).Log("msg", "Error executing sdiag", "err", stderr.String(), "out", stdout.String())
			return "", err
		}
	}
	return stdout.String(), nil
}

// Extract the relevant metrics from the sdiag output
func ParseSchedulerMetrics(input string, logger log.Logger) *SchedulerMetrics {
	var sm SchedulerMetrics
	lines := strings.Split(input, "\n")
	// Guard variables to check for string repetitions in the output of sdiag
	// (two occurencies of the following strings: 'Last cycle', 'Mean cycle')
	in_backfill := false
	for _, line := range lines {
		if strings.Contains(line, "Backfilling stats") {
			in_backfill = true
		}
		if strings.Contains(line, "Remote Procedure Call statistics") {
			break
		}
		if strings.Contains(line, ":") {
			state := strings.Split(line, ":")[0]
			value, err := strconv.ParseFloat(strings.TrimSpace(strings.Split(line, ":")[1]), 64)
			if err != nil {
				level.Debug(logger).Log("msg", "Unable to parse value from line", "line", line)
				continue
			}
			st := regexp.MustCompile(`(?i)^Server thread`)
			aqs := regexp.MustCompile(`(?i)^Agent queue size`)
			ac := regexp.MustCompile(`(?i)^Agent count`)
			atc := regexp.MustCompile(`(?i)^Agent thread count`)
			dbd := regexp.MustCompile(`(?i)^DBD Agent`)
			lc := regexp.MustCompile(`(?i)^[\s]+Last cycle$`)
			mc := regexp.MustCompile(`(?i)^[\s]+Mean cycle$`)
			mdc := regexp.MustCompile(`(?i)^[\s]+Mean depth cycle`)
			cpm := regexp.MustCompile(`(?i)^[\s]+Cycles per`)
			lql := regexp.MustCompile(`(?i)^[\s]+Last queue length`)
			dpm := regexp.MustCompile(`(?i)^[\s]+Depth Mean`)
			ldc := regexp.MustCompile(`(?i)^[\s]+Last depth cycle`)
			qlm := regexp.MustCompile(`(?i)^[\s]+Queue length Mean`)
			lts := regexp.MustCompile(`(?i)^[\s]+Last table size`)
			mts := regexp.MustCompile(`(?i)^[\s]+Mean table size`)
			tbs := regexp.MustCompile(`(?i)^[\s]+Total backfilled jobs \(since last slurm start\)`)
			tbc := regexp.MustCompile(`(?i)^[\s]+Total backfilled jobs \(since last stats cycle start\)`)
			tbh := regexp.MustCompile(`(?i)^[\s]+Total backfilled heterogeneous job components`)

			switch {
			case st.MatchString(state):
				sm.threads = value
			case aqs.MatchString(state):
				sm.agent_queue_size = value
			case ac.MatchString(state):
				sm.agent_count = value
			case atc.MatchString(state):
				sm.agent_thread_count = value
			case dbd.MatchString(state):
				sm.dbd_queue_size = value
			case lc.MatchString(state):
				if in_backfill {
					sm.backfill_last_cycle = value
				} else {
					sm.last_cycle = value
				}
			case mc.MatchString(state):
				if in_backfill {
					sm.backfill_mean_cycle = value
				} else {
					sm.mean_cycle = value
				}
			case mdc.MatchString(state):
				sm.mean_depth_cycle = value
			case cpm.MatchString(state):
				sm.cycle_per_minute = value
			case lql.MatchString(state):
				if in_backfill {
					sm.backfill_last_queue_length = value
				} else {
					sm.last_queue_length = value
				}
			case dpm.MatchString(state):
				if strings.Contains(line, "try sched") {
					sm.backfill_depth_mean_try_sched = value
				} else if strings.Contains(line, "try depth") {
					sm.backfill_depth_mean_try_depth = value
				} else {
					sm.backfill_depth_mean = value
				}
			case ldc.MatchString(state):
				if strings.Contains(line, "try sched") {
					sm.backfill_last_depth_cycle_try_sched = value
				} else {
					sm.backfill_last_depth_cycle = value
				}
			case qlm.MatchString(state):
				sm.backfill_queue_length_mean = value
			case lts.MatchString(state):
				sm.backfill_last_table_size = value
			case mts.MatchString(state):
				sm.backfill_mean_table_size = value
			case tbs.MatchString(state):
				sm.total_backfilled_jobs_since_start = value
			case tbc.MatchString(state):
				sm.total_backfilled_jobs_since_cycle = value
			case tbh.MatchString(state):
				sm.total_backfilled_heterogeneous = value
			}
		}
	}
	rpc_stats := ParseRpcStats(lines)
	sm.rpc_stats_count = rpc_stats[0]
	sm.rpc_stats_avg_time = rpc_stats[1]
	sm.rpc_stats_total_time = rpc_stats[2]
	sm.user_rpc_stats_count = rpc_stats[3]
	sm.user_rpc_stats_avg_time = rpc_stats[4]
	sm.user_rpc_stats_total_time = rpc_stats[5]
	return &sm
}

// Helper function to split a single line from the sdiag output
func SplitColonValueToFloat(input string) float64 {
	str := strings.Split(input, ":")
	if len(str) == 1 {
		return 0
	} else {
		rvalue := strings.TrimSpace(str[1])
		flt, _ := strconv.ParseFloat(rvalue, 64)
		return flt
	}
}

// Helper function to return RPC stats from sdiag output
func ParseRpcStats(lines []string) []map[string]float64 {
	count_stats := make(map[string]float64)
	avg_stats := make(map[string]float64)
	total_stats := make(map[string]float64)
	user_count_stats := make(map[string]float64)
	user_avg_stats := make(map[string]float64)
	user_total_stats := make(map[string]float64)

	in_rpc := false
	in_rpc_per_user := false

	stat_line_re := regexp.MustCompile(`^\s*([A-Za-z0-9_]*).*count:([0-9]*)\s*ave_time:([0-9]*)\s\s*total_time:([0-9]*)\s*$`)

	for _, line := range lines {
		if strings.Contains(line, "Remote Procedure Call statistics by message type") {
			in_rpc = true
			in_rpc_per_user = false
		} else if strings.Contains(line, "Remote Procedure Call statistics by user") {
			in_rpc = false
			in_rpc_per_user = true
		}
		if in_rpc || in_rpc_per_user {
			re_match := stat_line_re.FindAllStringSubmatch(line, -1)
			if re_match != nil {
				re_match_first := re_match[0]
				if in_rpc {
					count_stats[re_match_first[1]], _ = strconv.ParseFloat(re_match_first[2], 64)
					avg_stats[re_match_first[1]], _ = strconv.ParseFloat(re_match_first[3], 64)
					total_stats[re_match_first[1]], _ = strconv.ParseFloat(re_match_first[4], 64)
				} else if in_rpc_per_user {
					user_count_stats[re_match_first[1]], _ = strconv.ParseFloat(re_match_first[2], 64)
					user_avg_stats[re_match_first[1]], _ = strconv.ParseFloat(re_match_first[3], 64)
					user_total_stats[re_match_first[1]], _ = strconv.ParseFloat(re_match_first[4], 64)
				}
			}
		}
	}

	rpc_stats_final := []map[string]float64{
		count_stats,
		avg_stats,
		total_stats,
		user_count_stats,
		user_avg_stats,
		user_total_stats,
	}
	return rpc_stats_final
}

// Returns the scheduler metrics
func SchedulerGetMetrics(logger log.Logger) (*SchedulerMetrics, error) {
	data, err := SchedulerData(logger)
	if err != nil {
		return &SchedulerMetrics{}, err
	}
	return ParseSchedulerMetrics(data, logger), nil
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

// Collector strcture
type SchedulerCollector struct {
	threads                             *prometheus.Desc
	agent_queue_size                    *prometheus.Desc
	agent_count                         *prometheus.Desc
	agent_thread_count                  *prometheus.Desc
	dbd_queue_size                      *prometheus.Desc
	last_cycle                          *prometheus.Desc
	mean_cycle                          *prometheus.Desc
	mean_depth_cycle                    *prometheus.Desc
	cycle_per_minute                    *prometheus.Desc
	last_queue_length                   *prometheus.Desc
	backfill_last_cycle                 *prometheus.Desc
	backfill_mean_cycle                 *prometheus.Desc
	backfill_depth_mean                 *prometheus.Desc
	backfill_depth_mean_try_sched       *prometheus.Desc
	backfill_depth_mean_try_depth       *prometheus.Desc
	backfill_last_depth_cycle           *prometheus.Desc
	backfill_last_depth_cycle_try_sched *prometheus.Desc
	backfill_last_queue_length          *prometheus.Desc
	backfill_queue_length_mean          *prometheus.Desc
	backfill_last_table_size            *prometheus.Desc
	backfill_mean_table_size            *prometheus.Desc
	total_backfilled_jobs_since_start   *prometheus.Desc
	total_backfilled_jobs_since_cycle   *prometheus.Desc
	total_backfilled_heterogeneous      *prometheus.Desc
	rpc_stats_count                     *prometheus.Desc
	rpc_stats_avg_time                  *prometheus.Desc
	rpc_stats_total_time                *prometheus.Desc
	user_rpc_stats_count                *prometheus.Desc
	user_rpc_stats_avg_time             *prometheus.Desc
	user_rpc_stats_total_time           *prometheus.Desc
	logger                              log.Logger
}

// Send all metric descriptions
func (c *SchedulerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.threads
	ch <- c.agent_queue_size
	ch <- c.agent_count
	ch <- c.agent_thread_count
	ch <- c.dbd_queue_size
	ch <- c.last_cycle
	ch <- c.mean_cycle
	ch <- c.mean_depth_cycle
	ch <- c.cycle_per_minute
	ch <- c.last_queue_length
	ch <- c.backfill_last_cycle
	ch <- c.backfill_mean_cycle
	ch <- c.backfill_depth_mean
	ch <- c.backfill_depth_mean_try_sched
	ch <- c.backfill_depth_mean_try_depth
	ch <- c.backfill_last_depth_cycle
	ch <- c.backfill_last_depth_cycle_try_sched
	ch <- c.backfill_last_queue_length
	ch <- c.backfill_queue_length_mean
	ch <- c.backfill_last_table_size
	ch <- c.backfill_mean_table_size
	ch <- c.total_backfilled_jobs_since_start
	ch <- c.total_backfilled_jobs_since_cycle
	ch <- c.total_backfilled_heterogeneous
	ch <- c.rpc_stats_count
	ch <- c.rpc_stats_avg_time
	ch <- c.rpc_stats_total_time
	ch <- c.user_rpc_stats_count
	ch <- c.user_rpc_stats_avg_time
	ch <- c.user_rpc_stats_total_time
}

// Send the values of all metrics
func (sc *SchedulerCollector) Collect(ch chan<- prometheus.Metric) {
	var timeout, errorMetric float64
	sm, err := SchedulerGetMetrics(sc.logger)
	if err == context.DeadlineExceeded {
		timeout = 1
	} else if err != nil {
		errorMetric = 1
	}
	ch <- prometheus.MustNewConstMetric(sc.threads, prometheus.GaugeValue, sm.threads)
	ch <- prometheus.MustNewConstMetric(sc.agent_queue_size, prometheus.GaugeValue, sm.agent_queue_size)
	ch <- prometheus.MustNewConstMetric(sc.agent_count, prometheus.GaugeValue, sm.agent_count)
	ch <- prometheus.MustNewConstMetric(sc.agent_thread_count, prometheus.GaugeValue, sm.agent_thread_count)
	ch <- prometheus.MustNewConstMetric(sc.dbd_queue_size, prometheus.GaugeValue, sm.dbd_queue_size)
	ch <- prometheus.MustNewConstMetric(sc.last_cycle, prometheus.GaugeValue, sm.last_cycle)
	ch <- prometheus.MustNewConstMetric(sc.mean_cycle, prometheus.GaugeValue, sm.mean_cycle)
	ch <- prometheus.MustNewConstMetric(sc.mean_depth_cycle, prometheus.GaugeValue, sm.mean_depth_cycle)
	ch <- prometheus.MustNewConstMetric(sc.cycle_per_minute, prometheus.GaugeValue, sm.cycle_per_minute)
	ch <- prometheus.MustNewConstMetric(sc.last_queue_length, prometheus.GaugeValue, sm.last_queue_length)
	ch <- prometheus.MustNewConstMetric(sc.backfill_last_cycle, prometheus.GaugeValue, sm.backfill_last_cycle)
	ch <- prometheus.MustNewConstMetric(sc.backfill_mean_cycle, prometheus.GaugeValue, sm.backfill_mean_cycle)
	ch <- prometheus.MustNewConstMetric(sc.backfill_depth_mean, prometheus.GaugeValue, sm.backfill_depth_mean)
	ch <- prometheus.MustNewConstMetric(sc.backfill_depth_mean_try_sched, prometheus.GaugeValue, sm.backfill_depth_mean_try_sched)
	ch <- prometheus.MustNewConstMetric(sc.backfill_depth_mean_try_depth, prometheus.GaugeValue, sm.backfill_depth_mean_try_depth)
	ch <- prometheus.MustNewConstMetric(sc.backfill_last_depth_cycle, prometheus.GaugeValue, sm.backfill_last_depth_cycle)
	ch <- prometheus.MustNewConstMetric(sc.backfill_last_depth_cycle_try_sched, prometheus.GaugeValue, sm.backfill_last_depth_cycle_try_sched)
	ch <- prometheus.MustNewConstMetric(sc.backfill_last_queue_length, prometheus.GaugeValue, sm.backfill_last_queue_length)
	ch <- prometheus.MustNewConstMetric(sc.backfill_queue_length_mean, prometheus.GaugeValue, sm.backfill_queue_length_mean)
	ch <- prometheus.MustNewConstMetric(sc.backfill_last_table_size, prometheus.GaugeValue, sm.backfill_last_table_size)
	ch <- prometheus.MustNewConstMetric(sc.backfill_mean_table_size, prometheus.GaugeValue, sm.backfill_mean_table_size)
	ch <- prometheus.MustNewConstMetric(sc.total_backfilled_jobs_since_start, prometheus.GaugeValue, sm.total_backfilled_jobs_since_start)
	ch <- prometheus.MustNewConstMetric(sc.total_backfilled_jobs_since_cycle, prometheus.GaugeValue, sm.total_backfilled_jobs_since_cycle)
	ch <- prometheus.MustNewConstMetric(sc.total_backfilled_heterogeneous, prometheus.GaugeValue, sm.total_backfilled_heterogeneous)
	for rpc_type, value := range sm.rpc_stats_count {
		ch <- prometheus.MustNewConstMetric(sc.rpc_stats_count, prometheus.CounterValue, value, rpc_type)
	}
	for rpc_type, value := range sm.rpc_stats_avg_time {
		ch <- prometheus.MustNewConstMetric(sc.rpc_stats_avg_time, prometheus.GaugeValue, value, rpc_type)
	}
	for rpc_type, value := range sm.rpc_stats_total_time {
		ch <- prometheus.MustNewConstMetric(sc.rpc_stats_total_time, prometheus.CounterValue, value, rpc_type)
	}
	for user, value := range sm.user_rpc_stats_count {
		ch <- prometheus.MustNewConstMetric(sc.user_rpc_stats_count, prometheus.CounterValue, value, user)
	}
	for user, value := range sm.user_rpc_stats_avg_time {
		ch <- prometheus.MustNewConstMetric(sc.user_rpc_stats_avg_time, prometheus.GaugeValue, value, user)
	}
	for user, value := range sm.user_rpc_stats_total_time {
		ch <- prometheus.MustNewConstMetric(sc.user_rpc_stats_total_time, prometheus.CounterValue, value, user)
	}
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errorMetric, "scheduler")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, timeout, "scheduler")
}

// Returns the Slurm scheduler collector, used to register with the prometheus client
func NewSchedulerCollector(logger log.Logger) *SchedulerCollector {

	rpc_stats_labels := []string{"operation"}
	user_rpc_stats_labels := []string{"user"}
	return &SchedulerCollector{
		threads: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "threads"),
			"Information provided by the Slurm sdiag command, number of scheduler threads ",
			nil,
			nil),
		agent_queue_size: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "agent_queue_size"),
			"Information provided by the Slurm sdiag command, queued outgoing RPC requests",
			nil,
			nil),
		agent_count: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "agent_count"),
			"Information provided by the Slurm sdiag command, number of agent threads",
			nil,
			nil),
		agent_thread_count: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "agent_thread_count"),
			"Information provided by the Slurm sdiag command, active agent threads",
			nil,
			nil),
		dbd_queue_size: prometheus.NewDesc(
			"slurm_scheduler_dbd_queue_size",
			"Information provided by the Slurm sdiag command, length of the DBD agent queue",
			nil,
			nil),
		last_cycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "last_cycle"),
			"Information provided by the Slurm sdiag command, scheduler last cycle time in (microseconds)",
			nil,
			nil),
		mean_cycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "mean_cycle"),
			"Information provided by the Slurm sdiag command, scheduler mean cycle time in (microseconds)",
			nil,
			nil),
		mean_depth_cycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "mean_depth_cycle"),
			"Information provided by the Slurm sdiag command, scheduler mean depth cycle time in (microseconds)",
			nil,
			nil),
		cycle_per_minute: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "cycle_per_minute"),
			"Information provided by the Slurm sdiag command, number scheduler cycles per minute",
			nil,
			nil),
		last_queue_length: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "last_queue_length"),
			"Information provided by the Slurm sdiag command, length of jobs pending queue",
			nil,
			nil),
		backfill_last_cycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfill_last_cycle"),
			"Information provided by the Slurm sdiag command, scheduler backfill last cycle time in (microseconds)",
			nil,
			nil),
		backfill_mean_cycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfill_mean_cycle"),
			"Information provided by the Slurm sdiag command, scheduler backfill mean cycle time in (microseconds)",
			nil,
			nil),
		backfill_depth_mean: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfill_depth_mean"),
			"Information provided by the Slurm sdiag command, scheduler backfill mean depth",
			nil,
			nil),
		backfill_depth_mean_try_sched: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfill_depth_mean_try_sched"),
			"Information provided by the Slurm sdiag command, scheduler backfill mean depth (try sched)",
			nil,
			nil),
		backfill_depth_mean_try_depth: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfill_depth_mean_try_depth"),
			"Information provided by the Slurm sdiag command, scheduler backfill mean depth (try depth)",
			nil,
			nil),
		backfill_last_depth_cycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfill_last_depth_cycle"),
			"Information provided by the Slurm sdiag command, scheduler backfill last depth cycle",
			nil,
			nil),
		backfill_last_depth_cycle_try_sched: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfill_last_depth_cycle_try_sched"),
			"Information provided by the Slurm sdiag command, scheduler backfill last depth cycle (try sched)",
			nil,
			nil),
		backfill_last_queue_length: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfill_last_queue_length"),
			"Information provided by the Slurm sdiag command, scheduler backfill last queye length",
			nil,
			nil),
		backfill_queue_length_mean: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfill_queue_length_mean"),
			"Information provided by the Slurm sdiag command, scheduler backfill queue length mean",
			nil,
			nil),
		backfill_last_table_size: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfill_last_table_size"),
			"Information provided by the Slurm sdiag command, scheduler backfill last table size",
			nil,
			nil),
		backfill_mean_table_size: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfill_mean_table_size"),
			"Information provided by the Slurm sdiag command, scheduler backfill mean table size",
			nil,
			nil),
		total_backfilled_jobs_since_start: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfilled_jobs_since_start_total"),
			"Information provided by the Slurm sdiag command, number of jobs started thanks to backfilling since last slurm start",
			nil,
			nil),
		total_backfilled_jobs_since_cycle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfilled_jobs_since_cycle_total"),
			"Information provided by the Slurm sdiag command, number of jobs started thanks to backfilling since last time stats where reset",
			nil,
			nil),
		total_backfilled_heterogeneous: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "scheduler", "backfilled_heterogeneous_total"),
			"Information provided by the Slurm sdiag command, number of heterogeneous job components started thanks to backfilling since last Slurm start",
			nil,
			nil),
		rpc_stats_count: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "rpc", "stats_count"),
			"Information provided by the Slurm sdiag command, rpc count statistic",
			rpc_stats_labels,
			nil),
		rpc_stats_avg_time: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "rpc", "stats_time_avg"),
			"Information provided by the Slurm sdiag command, rpc average time statistic in microseconds",
			rpc_stats_labels,
			nil),
		rpc_stats_total_time: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "rpc", "stats_time_total"),
			"Information provided by the Slurm sdiag command, rpc total time statistic in microseconds",
			rpc_stats_labels,
			nil),
		user_rpc_stats_count: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "user_rpc", "stats"),
			"Information provided by the Slurm sdiag command, rpc count statistic per user",
			user_rpc_stats_labels,
			nil),
		user_rpc_stats_avg_time: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "user_rpc", "stats_time_avg"),
			"Information provided by the Slurm sdiag command, rpc average time statistic per user in microseconds",
			user_rpc_stats_labels,
			nil),
		user_rpc_stats_total_time: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "user_rpc", "stats_time_total"),
			"Information provided by the Slurm sdiag command, rpc total time statistic per user in microseconds",
			user_rpc_stats_labels,
			nil),
		logger: log.With(logger, "collector", "scheduler"),
	}
}
