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
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

type PartitionCPUMetrics struct {
	cpuAlloc map[string]float64
	cpuIdle  map[string]float64
	cpuOther map[string]float64
	cpuTotal map[string]float64
}

func PartitionGetCPUMetrics(ignore []string, logger log.Logger) (*PartitionCPUMetrics, error) {
	cpuData, err := PartitionCPUsData(logger)
	if err != nil {
		return &PartitionCPUMetrics{}, err
	}
	return ParsePartitionCPUs(cpuData, ignore), nil
}

func ParsePartitionCPUs(input string, ignore []string) *PartitionCPUMetrics {
	var pm PartitionCPUMetrics
	cpuAlloc := make(map[string]float64)
	cpuIdle := make(map[string]float64)
	cpuOther := make(map[string]float64)
	cpuTotal := make(map[string]float64)

	cpulines := strings.Split(input, "\n")
	for _, line := range cpulines {
		items := strings.Split(strings.TrimSpace(string(line)), ",")
		if len(items) != 2 {
			continue
		}
		cpus := strings.Split(items[1], "/")
		if len(cpus) != 4 {
			continue
		}
		partition := items[0]
		if sliceContains(ignore, partition) {
			continue
		}
		alloc, _ := strconv.ParseFloat(cpus[0], 64)
		idle, _ := strconv.ParseFloat(cpus[1], 64)
		other, _ := strconv.ParseFloat(cpus[2], 64)
		total, _ := strconv.ParseFloat(cpus[3], 64)
		cpuAlloc[partition] = alloc
		cpuIdle[partition] = idle
		cpuOther[partition] = other
		cpuTotal[partition] = total
	}
	pm.cpuAlloc = cpuAlloc
	pm.cpuIdle = cpuIdle
	pm.cpuOther = cpuOther
	pm.cpuTotal = cpuTotal
	return &pm
}

func PartitionCPUsData(logger log.Logger) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*collectorTimeout)*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sinfo", "-a", "-h", "-o", "%R,%C")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			level.Error(logger).Log("msg", "Timeout executing sinfo for partitions")
			return "", ctx.Err()
		} else {
			level.Error(logger).Log("msg", "Error executing sinfo for partitions", "err", stderr.String(), "out", stdout.String())
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

func NewPartitionCPUsCollector(ignore []string, logger log.Logger) *PartitionCPUsCollector {
	labels := []string{"partition"}
	return &PartitionCPUsCollector{
		cpuAlloc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "cpus_alloc"),
			"Allocated CPUs", labels, nil),
		cpuIdle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "cpus_idle"),
			"Idle CPUs", labels, nil),
		cpuOther: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "cpus_other"),
			"Mix CPUs", labels, nil),
		cpuTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "cpus_total"),
			"Total CPUs", labels, nil),
		ignore: ignore,
		logger: log.With(logger, "collector", "partition_cpu"),
	}
}

type PartitionCPUsCollector struct {
	cpuAlloc *prometheus.Desc
	cpuIdle  *prometheus.Desc
	cpuOther *prometheus.Desc
	cpuTotal *prometheus.Desc
	ignore   []string
	logger   log.Logger
}

// Send all metric descriptions
func (pc *PartitionCPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pc.cpuAlloc
	ch <- pc.cpuIdle
	ch <- pc.cpuOther
	ch <- pc.cpuTotal
}
func (pc *PartitionCPUsCollector) Collect(ch chan<- prometheus.Metric) {
	var timeout, errorMetric float64
	pm, err := PartitionGetCPUMetrics(pc.ignore, pc.logger)
	if err == context.DeadlineExceeded {
		timeout = 1
	} else if err != nil {
		errorMetric = 1
	}
	for partition, count := range pm.cpuAlloc {
		ch <- prometheus.MustNewConstMetric(pc.cpuAlloc, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.cpuIdle {
		ch <- prometheus.MustNewConstMetric(pc.cpuIdle, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.cpuOther {
		ch <- prometheus.MustNewConstMetric(pc.cpuOther, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.cpuTotal {
		ch <- prometheus.MustNewConstMetric(pc.cpuTotal, prometheus.GaugeValue, count, partition)
	}
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errorMetric, "partition_cpu")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, timeout, "partition_cpu")
}
