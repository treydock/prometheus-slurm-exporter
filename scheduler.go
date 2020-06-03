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
	threads                           float64
	queue_size                        float64
	dbd_queue_size                    float64
	last_cycle                        float64
	mean_cycle                        float64
	cycle_per_minute                  float64
	backfill_last_cycle               float64
	backfill_mean_cycle               float64
	backfill_depth_mean               float64
	total_backfilled_jobs_since_start float64
	total_backfilled_jobs_since_cycle float64
	total_backfilled_heterogeneous    float64
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
func ParseSchedulerMetrics(input string) *SchedulerMetrics {
	var sm SchedulerMetrics
	lines := strings.Split(input, "\n")
	// Guard variables to check for string repetitions in the output of sdiag
	// (two occurencies of the following strings: 'Last cycle', 'Mean cycle')
	lc_count := 0
	mc_count := 0
	for _, line := range lines {
		if strings.Contains(line, ":") {
			state := strings.Split(line, ":")[0]
			st := regexp.MustCompile(`^Server thread`)
			qs := regexp.MustCompile(`^Agent queue`)
			dbd := regexp.MustCompile(`^DBD Agent`)
			lc := regexp.MustCompile(`^[\s]+Last cycle$`)
			mc := regexp.MustCompile(`^[\s]+Mean cycle$`)
			cpm := regexp.MustCompile(`^[\s]+Cycles per`)
			dpm := regexp.MustCompile(`^[\s]+Depth Mean$`)
			tbs := regexp.MustCompile(`^[\s]+Total backfilled jobs \(since last slurm start\)`)
			tbc := regexp.MustCompile(`^[\s]+Total backfilled jobs \(since last stats cycle start\)`)
			tbh := regexp.MustCompile(`^[\s]+Total backfilled heterogeneous job components`)
			switch {
			case st.MatchString(state) == true:
				sm.threads, _ = strconv.ParseFloat(strings.TrimSpace(strings.Split(line, ":")[1]), 64)
			case qs.MatchString(state) == true:
				sm.queue_size, _ = strconv.ParseFloat(strings.TrimSpace(strings.Split(line, ":")[1]), 64)
			case dbd.MatchString(state) == true:
				sm.dbd_queue_size, _ = strconv.ParseFloat(strings.TrimSpace(strings.Split(line, ":")[1]), 64)
			case lc.MatchString(state) == true:
				if lc_count == 0 {
					sm.last_cycle, _ = strconv.ParseFloat(strings.TrimSpace(strings.Split(line, ":")[1]), 64)
					lc_count = 1
				}
				if lc_count == 1 {
					sm.backfill_last_cycle, _ = strconv.ParseFloat(strings.TrimSpace(strings.Split(line, ":")[1]), 64)
				}
			case mc.MatchString(state) == true:
				if mc_count == 0 {
					sm.mean_cycle, _ = strconv.ParseFloat(strings.TrimSpace(strings.Split(line, ":")[1]), 64)
					mc_count = 1
				}
				if mc_count == 1 {
					sm.backfill_mean_cycle, _ = strconv.ParseFloat(strings.TrimSpace(strings.Split(line, ":")[1]), 64)
				}
			case cpm.MatchString(state) == true:
				sm.cycle_per_minute, _ = strconv.ParseFloat(strings.TrimSpace(strings.Split(line, ":")[1]), 64)
			case dpm.MatchString(state) == true:
				sm.backfill_depth_mean, _ = strconv.ParseFloat(strings.TrimSpace(strings.Split(line, ":")[1]), 64)
			case tbs.MatchString(state) == true:
				sm.total_backfilled_jobs_since_start, _ = strconv.ParseFloat(strings.TrimSpace(strings.Split(line, ":")[1]), 64)
			case tbc.MatchString(state) == true:
				sm.total_backfilled_jobs_since_cycle, _ = strconv.ParseFloat(strings.TrimSpace(strings.Split(line, ":")[1]), 64)
			case tbh.MatchString(state) == true:
				sm.total_backfilled_heterogeneous, _ = strconv.ParseFloat(strings.TrimSpace(strings.Split(line, ":")[1]), 64)
			}
		}
	}
	return &sm
}

// Returns the scheduler metrics
func SchedulerGetMetrics(logger log.Logger) (*SchedulerMetrics, error) {
	data, err := SchedulerData(logger)
	if err != nil {
		return &SchedulerMetrics{}, err
	}
	return ParseSchedulerMetrics(data), nil
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

// Collector strcture
type SchedulerCollector struct {
	threads                           *prometheus.Desc
	queue_size                        *prometheus.Desc
	dbd_queue_size                    *prometheus.Desc
	last_cycle                        *prometheus.Desc
	mean_cycle                        *prometheus.Desc
	cycle_per_minute                  *prometheus.Desc
	backfill_last_cycle               *prometheus.Desc
	backfill_mean_cycle               *prometheus.Desc
	backfill_depth_mean               *prometheus.Desc
	total_backfilled_jobs_since_start *prometheus.Desc
	total_backfilled_jobs_since_cycle *prometheus.Desc
	total_backfilled_heterogeneous    *prometheus.Desc
	logger                            log.Logger
}

// Send all metric descriptions
func (c *SchedulerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.threads
	ch <- c.queue_size
	ch <- c.dbd_queue_size
	ch <- c.last_cycle
	ch <- c.mean_cycle
	ch <- c.cycle_per_minute
	ch <- c.backfill_last_cycle
	ch <- c.backfill_mean_cycle
	ch <- c.backfill_depth_mean
	ch <- c.total_backfilled_jobs_since_start
	ch <- c.total_backfilled_jobs_since_cycle
	ch <- c.total_backfilled_heterogeneous
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
	ch <- prometheus.MustNewConstMetric(sc.queue_size, prometheus.GaugeValue, sm.queue_size)
	ch <- prometheus.MustNewConstMetric(sc.dbd_queue_size, prometheus.GaugeValue, sm.dbd_queue_size)
	ch <- prometheus.MustNewConstMetric(sc.last_cycle, prometheus.GaugeValue, sm.last_cycle)
	ch <- prometheus.MustNewConstMetric(sc.mean_cycle, prometheus.GaugeValue, sm.mean_cycle)
	ch <- prometheus.MustNewConstMetric(sc.cycle_per_minute, prometheus.GaugeValue, sm.cycle_per_minute)
	ch <- prometheus.MustNewConstMetric(sc.backfill_last_cycle, prometheus.GaugeValue, sm.backfill_last_cycle)
	ch <- prometheus.MustNewConstMetric(sc.backfill_mean_cycle, prometheus.GaugeValue, sm.backfill_mean_cycle)
	ch <- prometheus.MustNewConstMetric(sc.backfill_depth_mean, prometheus.GaugeValue, sm.backfill_depth_mean)
	ch <- prometheus.MustNewConstMetric(sc.total_backfilled_jobs_since_start, prometheus.GaugeValue, sm.total_backfilled_jobs_since_start)
	ch <- prometheus.MustNewConstMetric(sc.total_backfilled_jobs_since_cycle, prometheus.GaugeValue, sm.total_backfilled_jobs_since_cycle)
	ch <- prometheus.MustNewConstMetric(sc.total_backfilled_heterogeneous, prometheus.GaugeValue, sm.total_backfilled_heterogeneous)
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errorMetric, "scheduler")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, timeout, "scheduler")
}

// Returns the Slurm scheduler collector, used to register with the prometheus client
func NewSchedulerCollector(logger log.Logger) *SchedulerCollector {
	return &SchedulerCollector{
		threads: prometheus.NewDesc(
			"slurm_scheduler_threads",
			"Information provided by the Slurm sdiag command, number of scheduler threads ",
			nil,
			nil),
		queue_size: prometheus.NewDesc(
			"slurm_scheduler_queue_size",
			"Information provided by the Slurm sdiag command, length of the scheduler queue",
			nil,
			nil),
		dbd_queue_size: prometheus.NewDesc(
			"slurm_scheduler_dbd_queue_size",
			"Information provided by the Slurm sdiag command, length of the DBD agent queue",
			nil,
			nil),
		last_cycle: prometheus.NewDesc(
			"slurm_scheduler_last_cycle",
			"Information provided by the Slurm sdiag command, scheduler last cycle time in (microseconds)",
			nil,
			nil),
		mean_cycle: prometheus.NewDesc(
			"slurm_scheduler_mean_cycle",
			"Information provided by the Slurm sdiag command, scheduler mean cycle time in (microseconds)",
			nil,
			nil),
		cycle_per_minute: prometheus.NewDesc(
			"slurm_scheduler_cycle_per_minute",
			"Information provided by the Slurm sdiag command, number scheduler cycles per minute",
			nil,
			nil),
		backfill_last_cycle: prometheus.NewDesc(
			"slurm_scheduler_backfill_last_cycle",
			"Information provided by the Slurm sdiag command, scheduler backfill last cycle time in (microseconds)",
			nil,
			nil),
		backfill_mean_cycle: prometheus.NewDesc(
			"slurm_scheduler_backfill_mean_cycle",
			"Information provided by the Slurm sdiag command, scheduler backfill mean cycle time in (microseconds)",
			nil,
			nil),
		backfill_depth_mean: prometheus.NewDesc(
			"slurm_scheduler_backfill_depth_mean",
			"Information provided by the Slurm sdiag command, scheduler backfill mean depth",
			nil,
			nil),
		total_backfilled_jobs_since_start: prometheus.NewDesc(
			"slurm_scheduler_backfilled_jobs_since_start_total",
			"Information provided by the Slurm sdiag command, number of jobs started thanks to backfilling since last slurm start",
			nil,
			nil),
			"slurm_scheduler_backfilled_jobs_since_cycle_total",
			"Information provided by the Slurm sdiag command, number of jobs started thanks to backfilling since last time stats where reset",
			nil,
			nil),
		total_backfilled_heterogeneous: prometheus.NewDesc(
			"slurm_scheduler_backfilled_heterogeneous_total",
			"Information provided by the Slurm sdiag command, number of heterogeneous job components started thanks to backfilling since last Slurm start",
			nil,
			nil),
		logger: log.With(logger, "collector", "scheduler"),
	}
}
