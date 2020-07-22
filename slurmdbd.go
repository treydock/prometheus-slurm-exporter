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
	"gopkg.in/alecthomas/kingpin.v2"
)

var slurmdbEnable = kingpin.Flag("collector.slurmdbd",
	"Enable the slurmdbd collector").Default("false").Bool()
var slurmdbdUseSudo = kingpin.Flag("collector.slurmdbd.use-sudo",
	"Use sudo when executing 'sacctmgr show stats'").Default("false").Bool()

/*
 * Execute the Slurm 'sacctmgr show stats' command to read the current statistics
 * from the Slurmdbd daemon. It will be repreatedly called by the
 * collector.
 */

// Basic metrics for the scheduler
type SlurmdbdMetrics struct {
	internal_rollup_last_cycle     float64
	internal_rollup_mean_cycle     float64
	cluster_rollup_hour_last_cycle map[string]float64
	cluster_rollup_hour_mean_cycle map[string]float64
	cluster_rollup_day_last_cycle  map[string]float64
	cluster_rollup_day_mean_cycle  map[string]float64
	rpc_stats_count                map[string]float64
	rpc_stats_avg_time             map[string]float64
	rpc_stats_total_time           map[string]float64
	user_rpc_stats_count           map[string]float64
	user_rpc_stats_avg_time        map[string]float64
	user_rpc_stats_total_time      map[string]float64
}

// Execute the sdiag command and return its output
func SlurmdbdData(logger log.Logger) (string, error) {
	var sacctmgr_name string
	var sacctmgr_args []string
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*collectorTimeout)*time.Second)
	defer cancel()
	if *slurmdbdUseSudo {
		sacctmgr_name = "sudo"
		sacctmgr_args = []string{"sacctmgr", "show", "stats"}
	} else {
		sacctmgr_name = "sacctmgr"
		sacctmgr_args = []string{"show", "stats"}
	}
	cmd := exec.CommandContext(ctx, sacctmgr_name, sacctmgr_args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			level.Error(logger).Log("msg", "Timeout executing sacctmgr show stats")
			return "", ctx.Err()
		} else {
			level.Error(logger).Log("msg", "Error executing sacctmgr show stats", "err", stderr.String(), "out", stdout.String())
			return "", err
		}
	}
	return stdout.String(), nil
}

// Extract the relevant metrics from the sdiag output
func ParseSlurmdbdMetrics(input string, logger log.Logger) *SlurmdbdMetrics {
	var sm SlurmdbdMetrics
	cluster_rollup_hour_last_cycle := make(map[string]float64)
	cluster_rollup_hour_mean_cycle := make(map[string]float64)
	cluster_rollup_day_last_cycle := make(map[string]float64)
	cluster_rollup_day_mean_cycle := make(map[string]float64)
	lines := strings.Split(input, "\n")
	internal := false
	cluster := ""
	rollupInterval := ""
	for _, line := range lines {
		clusterRegex := regexp.MustCompile(`^Cluster '(.+)' rollup statistics`)
		if strings.HasPrefix(line, "Internal DBD") {
			internal = true
			cluster = ""
		} else if strings.HasPrefix(line, "Cluster") {
			clusterMatch := clusterRegex.FindStringSubmatch(line)
			if clusterMatch != nil {
				internal = false
				cluster = clusterMatch[1]
			}
		} else if strings.HasPrefix(line, "Hour") {
			rollupInterval = "hour"
		} else if strings.HasPrefix(line, "Day") {
			rollupInterval = "day"
		}

		if strings.Contains(line, ":") {
			items := strings.Split(line, ":")
			if len(items) != 2 {
				continue
			}
			state := items[0]
			value, err := strconv.ParseFloat(strings.TrimSpace(items[1]), 64)
			if err != nil {
				level.Error(logger).Log("msg", "Error parsing float value", "line", line)
				continue
			}
			lc := regexp.MustCompile(`^[\s]+Last cycle$`)
			mc := regexp.MustCompile(`^[\s]+Mean cycle$`)

			switch {
			case lc.MatchString(state):
				if internal {
					sm.internal_rollup_last_cycle = value
				} else if cluster != "" {
					if rollupInterval == "hour" {
						cluster_rollup_hour_last_cycle[cluster] = value
					} else if rollupInterval == "day" {
						cluster_rollup_day_last_cycle[cluster] = value
					}
				}
			case mc.MatchString(state):
				if internal {
					sm.internal_rollup_mean_cycle = value
				} else if cluster != "" {
					if rollupInterval == "hour" {
						cluster_rollup_hour_mean_cycle[cluster] = value
					} else if rollupInterval == "day" {
						cluster_rollup_day_mean_cycle[cluster] = value
					}
				}
			}
		}
	}
	sm.cluster_rollup_hour_last_cycle = cluster_rollup_hour_last_cycle
	sm.cluster_rollup_day_last_cycle = cluster_rollup_day_last_cycle
	sm.cluster_rollup_hour_mean_cycle = cluster_rollup_hour_mean_cycle
	sm.cluster_rollup_day_mean_cycle = cluster_rollup_day_mean_cycle
	rpc_stats := ParseRpcStats(lines)
	sm.rpc_stats_count = rpc_stats[0]
	sm.rpc_stats_avg_time = rpc_stats[1]
	sm.rpc_stats_total_time = rpc_stats[2]
	sm.user_rpc_stats_count = rpc_stats[3]
	sm.user_rpc_stats_avg_time = rpc_stats[4]
	sm.user_rpc_stats_total_time = rpc_stats[5]
	return &sm
}

// Returns the scheduler metrics
func SlurmdbdGetMetrics(logger log.Logger) (*SlurmdbdMetrics, error) {
	data, err := SlurmdbdData(logger)
	if err != nil {
		return &SlurmdbdMetrics{}, err
	}
	return ParseSlurmdbdMetrics(data, logger), nil
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

// Collector strcture
type SlurmdbdCollector struct {
	internal_rollup_last_cycle     *prometheus.Desc
	internal_rollup_mean_cycle     *prometheus.Desc
	cluster_rollup_hour_last_cycle *prometheus.Desc
	cluster_rollup_hour_mean_cycle *prometheus.Desc
	cluster_rollup_day_last_cycle  *prometheus.Desc
	cluster_rollup_day_mean_cycle  *prometheus.Desc
	rpc_stats_count                *prometheus.Desc
	rpc_stats_avg_time             *prometheus.Desc
	rpc_stats_total_time           *prometheus.Desc
	user_rpc_stats_count           *prometheus.Desc
	user_rpc_stats_avg_time        *prometheus.Desc
	user_rpc_stats_total_time      *prometheus.Desc
	logger                         log.Logger
}

// Send all metric descriptions
func (c *SlurmdbdCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.internal_rollup_last_cycle
	ch <- c.internal_rollup_mean_cycle
	ch <- c.cluster_rollup_hour_last_cycle
	ch <- c.cluster_rollup_hour_mean_cycle
	ch <- c.cluster_rollup_day_last_cycle
	ch <- c.cluster_rollup_day_mean_cycle
	ch <- c.rpc_stats_count
	ch <- c.rpc_stats_avg_time
	ch <- c.rpc_stats_total_time
	ch <- c.user_rpc_stats_count
	ch <- c.user_rpc_stats_avg_time
	ch <- c.user_rpc_stats_total_time
}

// Send the values of all metrics
func (sc *SlurmdbdCollector) Collect(ch chan<- prometheus.Metric) {
	var timeout, errorMetric float64
	sm, err := SlurmdbdGetMetrics(sc.logger)
	if err == context.DeadlineExceeded {
		timeout = 1
	} else if err != nil {
		errorMetric = 1
	}
	ch <- prometheus.MustNewConstMetric(sc.internal_rollup_last_cycle, prometheus.GaugeValue, sm.internal_rollup_last_cycle)
	ch <- prometheus.MustNewConstMetric(sc.internal_rollup_mean_cycle, prometheus.GaugeValue, sm.internal_rollup_mean_cycle)
	for cluster, value := range sm.cluster_rollup_hour_last_cycle {
		ch <- prometheus.MustNewConstMetric(sc.cluster_rollup_hour_last_cycle, prometheus.GaugeValue, value, cluster)
	}
	for cluster, value := range sm.cluster_rollup_hour_mean_cycle {
		ch <- prometheus.MustNewConstMetric(sc.cluster_rollup_hour_mean_cycle, prometheus.GaugeValue, value, cluster)
	}
	for cluster, value := range sm.cluster_rollup_day_last_cycle {
		ch <- prometheus.MustNewConstMetric(sc.cluster_rollup_day_last_cycle, prometheus.GaugeValue, value, cluster)
	}
	for cluster, value := range sm.cluster_rollup_day_mean_cycle {
		ch <- prometheus.MustNewConstMetric(sc.cluster_rollup_day_mean_cycle, prometheus.GaugeValue, value, cluster)
	}
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
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errorMetric, "slurmdbd")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, timeout, "slurmdbd")
}

// Returns the Slurm scheduler collector, used to register with the prometheus client
func NewSlurmdbdCollector(logger log.Logger) *SlurmdbdCollector {
	cluster_rollup_labels := []string{"cluster"}
	rpc_stats_labels := []string{"operation"}
	user_rpc_stats_labels := []string{"user"}
	return &SlurmdbdCollector{
		internal_rollup_last_cycle: prometheus.NewDesc(
			"slurm_dbd_internal_rollup_last_cycle",
			"Information provided by the Slurm sacctmgr show stats command, internal db rollup last cycle (microseconds)",
			nil,
			nil),
		internal_rollup_mean_cycle: prometheus.NewDesc(
			"slurm_dbd_internal_rollup_mean_cycle",
			"Information provided by the Slurm sacctmgr show stats command, internal db rollup mean cycle (microseconds)",
			nil,
			nil),
		cluster_rollup_hour_last_cycle: prometheus.NewDesc(
			"slurm_dbd_cluster_rollup_hour_last_cycle",
			"Information provided by the Slurm sacctmgr show stats command, cluster db hour rollup last cycle (microseconds)",
			cluster_rollup_labels,
			nil),
		cluster_rollup_hour_mean_cycle: prometheus.NewDesc(
			"slurm_dbd_cluster_rollup_hour_mean_cycle",
			"Information provided by the Slurm sacctmgr show stats command, cluster db hour rollup mean cycle (microseconds)",
			cluster_rollup_labels,
			nil),
		cluster_rollup_day_last_cycle: prometheus.NewDesc(
			"slurm_dbd_cluster_rollup_day_last_cycle",
			"Information provided by the Slurm sacctmgr show stats command, cluster db day rollup last cycle (microseconds)",
			cluster_rollup_labels,
			nil),
		cluster_rollup_day_mean_cycle: prometheus.NewDesc(
			"slurm_dbd_cluster_rollup_day_mean_cycle",
			"Information provided by the Slurm sacctmgr show stats command, cluster db day rollup mean cycle (microseconds)",
			cluster_rollup_labels,
			nil),
		rpc_stats_count: prometheus.NewDesc(
			"slurm_dbd_rpc_stats_count",
			"Information provided by the Slurm sacctmgr show stats command, rpc count statistic",
			rpc_stats_labels,
			nil),
		rpc_stats_avg_time: prometheus.NewDesc(
			"slurm_dbd_rpc_stats_avg_time",
			"Information provided by the Slurm sacctmgr show stats command, rpc average time statistic in microseconds",
			rpc_stats_labels,
			nil),
		rpc_stats_total_time: prometheus.NewDesc(
			"slurm_dbd_rpc_stats_total_time",
			"Information provided by the Slurm sacctmgr show stats command, rpc total time statistic in microseconds",
			rpc_stats_labels,
			nil),
		user_rpc_stats_count: prometheus.NewDesc(
			"slurm_dbd_user_rpc_stats",
			"Information provided by the Slurm sacctmgr show stats command, rpc count statistic per user",
			user_rpc_stats_labels,
			nil),
		user_rpc_stats_avg_time: prometheus.NewDesc(
			"slurm_dbd_user_rpc_stats_avg_time",
			"Information provided by the Slurm sacctmgr show stats command, rpc average time statistic per user in microseconds",
			user_rpc_stats_labels,
			nil),
		user_rpc_stats_total_time: prometheus.NewDesc(
			"slurm_dbd_user_rpc_stats_total_time",
			"Information provided by the Slurm sacctmgr show stats command, rpc total time statistic per user in microseconds",
			user_rpc_stats_labels,
			nil),
		logger: log.With(logger, "collector", "slurmdbd"),
	}
}
