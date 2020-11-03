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
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/montanaflynn/stats"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	timeNowFunc = time.Now
	timeFormat  = "2006-01-02T15:04:05 MST"
)

type PartitionJobsMetrics struct {
	jobsTotal       map[string]float64
	jobsPending     map[string]float64
	jobsPendingDep  map[string]float64
	jobsRunning     map[string]float64
	jobsSuspended   map[string]float64
	jobsCancelled   map[string]float64
	jobsCompleting  map[string]float64
	jobsCompleted   map[string]float64
	jobsConfiguring map[string]float64
	jobsFailed      map[string]float64
	jobsTimeout     map[string]float64
	jobsPreempted   map[string]float64
	jobsNodeFail    map[string]float64
	jobsMedianWait  map[string]float64
	jobsAvgWait     map[string]float64
}

func PartitionGetJobsMetrics(partitions []string, ignore []string, logger log.Logger) (*PartitionJobsMetrics, error) {
	jobData, err := PartitionJobData(logger)
	if err != nil {
		return &PartitionJobsMetrics{}, err
	}
	return ParsePartitionJobsMetrics(jobData, partitions, ignore, logger), nil
}

func ParsePartitionJobsMetrics(jobData string, partitions []string, ignore []string, logger log.Logger) *PartitionJobsMetrics {
	var pm PartitionJobsMetrics

	ParsePartitionJobs(jobData, &pm, ignore, logger)

	for _, part := range partitions {
		if sliceContains(ignore, part) {
			continue
		}
		if _, ok := pm.jobsTotal[part]; !ok {
			pm.jobsTotal[part] = 0
		}
		if _, ok := pm.jobsPending[part]; !ok {
			pm.jobsPending[part] = 0
		}
		if _, ok := pm.jobsPendingDep[part]; !ok {
			pm.jobsPendingDep[part] = 0
		}
		if _, ok := pm.jobsRunning[part]; !ok {
			pm.jobsRunning[part] = 0
		}
		if _, ok := pm.jobsSuspended[part]; !ok {
			pm.jobsSuspended[part] = 0
		}
		if _, ok := pm.jobsCancelled[part]; !ok {
			pm.jobsCancelled[part] = 0
		}
		if _, ok := pm.jobsCompleting[part]; !ok {
			pm.jobsCompleting[part] = 0
		}
		if _, ok := pm.jobsCompleted[part]; !ok {
			pm.jobsCompleted[part] = 0
		}
		if _, ok := pm.jobsConfiguring[part]; !ok {
			pm.jobsConfiguring[part] = 0
		}
		if _, ok := pm.jobsFailed[part]; !ok {
			pm.jobsFailed[part] = 0
		}
		if _, ok := pm.jobsTimeout[part]; !ok {
			pm.jobsTimeout[part] = 0
		}
		if _, ok := pm.jobsPreempted[part]; !ok {
			pm.jobsPreempted[part] = 0
		}
		if _, ok := pm.jobsNodeFail[part]; !ok {
			pm.jobsNodeFail[part] = 0
		}
		if _, ok := pm.jobsMedianWait[part]; !ok {
			waitKey := fmt.Sprintf("%s|", part)
			pm.jobsMedianWait[waitKey] = 0
		}
		if _, ok := pm.jobsAvgWait[part]; !ok {
			waitKey := fmt.Sprintf("%s|", part)
			pm.jobsAvgWait[waitKey] = 0
		}
	}

	return &pm
}

func ParsePartitionJobs(input string, pm *PartitionJobsMetrics, ignore []string, logger log.Logger) {
	now := timeNowFunc()
	timeZone, _ := now.Zone()

	jobsTotal := make(map[string]float64)
	jobsPending := make(map[string]float64)
	jobsPendingDep := make(map[string]float64)
	jobsRunning := make(map[string]float64)
	jobsSuspended := make(map[string]float64)
	jobsCancelled := make(map[string]float64)
	jobsCompleting := make(map[string]float64)
	jobsCompleted := make(map[string]float64)
	jobsConfiguring := make(map[string]float64)
	jobsFailed := make(map[string]float64)
	jobsTimeout := make(map[string]float64)
	jobsPreempted := make(map[string]float64)
	jobsNodeFail := make(map[string]float64)
	jobsMedianWait := make(map[string]float64)
	jobsAvgWait := make(map[string]float64)
	waitTimes := make(map[string][]float64)

	lines := strings.Split(input, "\n")
	for _, line := range lines {
		items := strings.Split(line, "|")
		if len(items) < 5 {
			continue
		}
		partition := items[3]
		if sliceContains(ignore, partition) {
			continue
		}
		state := items[1]
		reason := items[2]
		jobsTotal[partition]++
		switch state {
		case "PENDING":
			if reason == "Dependency" {
				jobsPendingDep[partition]++
			} else {
				jobsPending[partition]++
			}
			waitKey := fmt.Sprintf("%s|%s", partition, reason)
			var w = waitTimes[waitKey]
			submitTime, err := time.Parse(timeFormat, fmt.Sprintf("%s %s", items[4], timeZone))
			if err != nil {
				level.Error(logger).Log("msg", "Unable to parse start time value", "value", items[4], "err", err)
			} else {
				wait := now.Sub(submitTime)
				w = append(w, wait.Seconds())
			}
			waitTimes[waitKey] = w
		case "RUNNING":
			jobsRunning[partition]++
		case "SUSPENDED":
			jobsSuspended[partition]++
		case "CANCELLED":
			jobsCancelled[partition]++
		case "COMPLETING":
			jobsCompleting[partition]++
		case "COMPLETED":
			jobsCompleted[partition]++
		case "CONFIGURING":
			jobsConfiguring[partition]++
		case "FAILED":
			jobsFailed[partition]++
		case "TIMEOUT":
			jobsTimeout[partition]++
		case "PREEMPTED":
			jobsPreempted[partition]++
		case "NODE_FAIL":
			jobsNodeFail[partition]++
		}
	}
	for partition, w := range waitTimes {
		if len(w) == 0 {
			continue
		}
		median, err := stats.Median(w)
		if err != nil {
			level.Error(logger).Log("msg", "Unable to caculate median wait for partition", "partition", partition, "err", err)
		} else {
			jobsMedianWait[partition] = median
		}
		avg, err := stats.Mean(w)
		if err != nil {
			level.Error(logger).Log("msg", "Unable to caculate average wait for partition", "partition", partition, "err", err)
		} else {
			jobsAvgWait[partition] = avg
		}
	}
	pm.jobsTotal = jobsTotal
	pm.jobsPending = jobsPending
	pm.jobsPendingDep = jobsPendingDep
	pm.jobsRunning = jobsRunning
	pm.jobsSuspended = jobsSuspended
	pm.jobsCancelled = jobsCancelled
	pm.jobsCompleting = jobsCompleting
	pm.jobsCompleted = jobsCompleted
	pm.jobsConfiguring = jobsConfiguring
	pm.jobsFailed = jobsFailed
	pm.jobsTimeout = jobsTimeout
	pm.jobsPreempted = jobsPreempted
	pm.jobsNodeFail = jobsNodeFail
	pm.jobsMedianWait = jobsMedianWait
	pm.jobsAvgWait = jobsAvgWait
}

func PartitionJobData(logger log.Logger) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*collectorTimeout)*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "squeue", "-a", "-r", "-h", "-P", "-o", "%i|%T|%r|%P|%V", "--states=all")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			level.Error(logger).Log("msg", "Timeout executing squeue")
			return "", ctx.Err()
		} else {
			level.Error(logger).Log("msg", "Error executing squeue", "err", stderr.String(), "out", stdout.String())
			return "", err
		}
	}
	return stdout.String(), nil
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewPartitionJobsCollector(partitions []string, ignore []string, logger log.Logger) *PartitionJobsCollector {
	labels := []string{"partition"}
	waitLabels := []string{"partition", "reason"}
	return &PartitionJobsCollector{
		jobsTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_total"),
			"Total jobs in the partition", labels, nil),
		jobsPending: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_pending"),
			"Pending jobs in the partition", labels, nil),
		jobsPendingDep: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_pending_dependency"),
			"Pending jobs because of dependency in the partition", labels, nil),
		jobsRunning: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_running"),
			"Running jobs in the cluster", labels, nil),
		jobsSuspended: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_suspended"),
			"Suspended jobs in the partition", labels, nil),
		jobsCancelled: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_cancelled"),
			"Cancelled jobs in the partition", labels, nil),
		jobsCompleting: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_completing"),
			"Completing jobs in the partition", labels, nil),
		jobsCompleted: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_completed"),
			"Completed jobs in the partition", labels, nil),
		jobsConfiguring: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_configuring"),
			"Configuring jobs in the partition", labels, nil),
		jobsFailed: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_failed"),
			"Number of failed jobs in the partition", labels, nil),
		jobsTimeout: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_timeout"),
			"Jobs stopped by timeout in the partition", labels, nil),
		jobsPreempted: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_preempted"),
			"Number of preempted jobs in the partition", labels, nil),
		jobsNodeFail: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_node_fail"),
			"Number of jobs stopped due to node fail in the partition", labels, nil),
		jobsMedianWait: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_median_wait_time_seconds"),
			"The median wait time for pending jobs in the partition", waitLabels, nil),
		jobsAvgWait: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "jobs_average_wait_time_seconds"),
			"The average wait time for pending jobs in the partition", waitLabels, nil),
		partitions: partitions,
		ignore:     ignore,
		logger:     log.With(logger, "collector", "partition_jobs"),
	}
}

type PartitionJobsCollector struct {
	jobsTotal       *prometheus.Desc
	jobsPending     *prometheus.Desc
	jobsPendingDep  *prometheus.Desc
	jobsRunning     *prometheus.Desc
	jobsSuspended   *prometheus.Desc
	jobsCancelled   *prometheus.Desc
	jobsCompleting  *prometheus.Desc
	jobsCompleted   *prometheus.Desc
	jobsConfiguring *prometheus.Desc
	jobsFailed      *prometheus.Desc
	jobsTimeout     *prometheus.Desc
	jobsPreempted   *prometheus.Desc
	jobsNodeFail    *prometheus.Desc
	jobsMedianWait  *prometheus.Desc
	jobsAvgWait     *prometheus.Desc
	partitions      []string
	ignore          []string
	logger          log.Logger
}

// Send all metric descriptions
func (pc *PartitionJobsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pc.jobsTotal
	ch <- pc.jobsPending
	ch <- pc.jobsPendingDep
	ch <- pc.jobsRunning
	ch <- pc.jobsSuspended
	ch <- pc.jobsCancelled
	ch <- pc.jobsCompleting
	ch <- pc.jobsCompleted
	ch <- pc.jobsConfiguring
	ch <- pc.jobsFailed
	ch <- pc.jobsTimeout
	ch <- pc.jobsPreempted
	ch <- pc.jobsNodeFail
	ch <- pc.jobsMedianWait
	ch <- pc.jobsAvgWait
}
func (pc *PartitionJobsCollector) Collect(ch chan<- prometheus.Metric) {
	var timeout, errorMetric float64
	pm, err := PartitionGetJobsMetrics(pc.partitions, pc.ignore, pc.logger)
	if err == context.DeadlineExceeded {
		timeout = 1
	} else if err != nil {
		errorMetric = 1
	}
	for partition, count := range pm.jobsTotal {
		ch <- prometheus.MustNewConstMetric(pc.jobsTotal, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.jobsPending {
		ch <- prometheus.MustNewConstMetric(pc.jobsPending, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.jobsPendingDep {
		ch <- prometheus.MustNewConstMetric(pc.jobsPendingDep, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.jobsRunning {
		ch <- prometheus.MustNewConstMetric(pc.jobsRunning, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.jobsSuspended {
		ch <- prometheus.MustNewConstMetric(pc.jobsSuspended, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.jobsCancelled {
		ch <- prometheus.MustNewConstMetric(pc.jobsCancelled, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.jobsCompleting {
		ch <- prometheus.MustNewConstMetric(pc.jobsCompleting, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.jobsCompleted {
		ch <- prometheus.MustNewConstMetric(pc.jobsCompleted, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.jobsConfiguring {
		ch <- prometheus.MustNewConstMetric(pc.jobsConfiguring, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.jobsFailed {
		ch <- prometheus.MustNewConstMetric(pc.jobsFailed, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.jobsTimeout {
		ch <- prometheus.MustNewConstMetric(pc.jobsTimeout, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.jobsPreempted {
		ch <- prometheus.MustNewConstMetric(pc.jobsPreempted, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.jobsNodeFail {
		ch <- prometheus.MustNewConstMetric(pc.jobsNodeFail, prometheus.GaugeValue, count, partition)
	}
	for key, count := range pm.jobsMedianWait {
		keys := strings.Split(key, "|")
		ch <- prometheus.MustNewConstMetric(pc.jobsMedianWait, prometheus.GaugeValue, count, keys[0], keys[1])
	}
	for key, count := range pm.jobsAvgWait {
		keys := strings.Split(key, "|")
		ch <- prometheus.MustNewConstMetric(pc.jobsAvgWait, prometheus.GaugeValue, count, keys[0], keys[1])
	}
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errorMetric, "partition_jobs")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, timeout, "partition_jobs")
}
