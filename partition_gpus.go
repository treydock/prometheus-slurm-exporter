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
	"github.com/prometheus/client_golang/prometheus"
)

type PartitionGPUMetrics struct {
	gpuAlloc map[string]float64
	gpuTotal map[string]float64
	gpuIdle  map[string]float64
}

func PartitionGetGPUMetrics(longestGRES int, ignore []string, logger log.Logger) (*PartitionGPUMetrics, error) {
	gpuData, err := PartitionGPUsData(longestGRES, logger)
	if err != nil {
		return &PartitionGPUMetrics{}, err
	}
	return ParsePartitionGPUs(gpuData, ignore), nil
}

func ParsePartitionGPUs(input string, ignore []string) *PartitionGPUMetrics {
	var pm PartitionGPUMetrics
	gpuAlloc := make(map[string]float64)
	gpuTotal := make(map[string]float64)
	gpuIdle := make(map[string]float64)

	lines := strings.Split(input, "\n")
	for _, line := range lines {
		data := strings.Fields(line)
		if len(data) != 3 {
			continue
		}
		part := data[0]
		if sliceContains(ignore, part) {
			continue
		}
		// This is how many GPUs are available on the node
		avail := parseGRES(data[1])

		// This is how many GPUs are allocated on the node
		alloc := parseGRES(data[2])

		gpuAlloc[part] += alloc
		gpuTotal[part] += avail
		gpuIdle[part] += (avail - alloc)
	}

	pm.gpuAlloc = gpuAlloc
	pm.gpuTotal = gpuTotal
	pm.gpuIdle = gpuIdle
	return &pm
}

func PartitionGPUsData(longestGRES int, logger log.Logger) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*collectorTimeout)*time.Second)
	defer cancel()
	format := fmt.Sprintf("--Format=partitionname:50,gres:%d,gresused:%d", longestGRES, longestGRES)
	cmd := exec.CommandContext(ctx, "sinfo", "-a", "-h", "--Node", format)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			level.Error(logger).Log("msg", "Timeout executing sinfo for GPUs")
			return "", ctx.Err()
		} else {
			level.Error(logger).Log("msg", "Error executing sinfo for GPUs", "err", stderr.String(), "out", stdout.String())
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

func NewPartitionGPUsCollector(longestGRES int, ignore []string, logger log.Logger) *PartitionGPUsCollector {
	labels := []string{"partition"}
	return &PartitionGPUsCollector{
		gpuAlloc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "gpus_alloc"),
			"Number of GPUs allocated in the partition", labels, nil),
		gpuTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "gpus_total"),
			"Number of GPUs in the partition", labels, nil),
		gpuIdle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "gpus_idle"),
			"Number of GPUs idle in the partition", labels, nil),
		longestGRES: longestGRES,
		ignore:      ignore,
		logger:      log.With(logger, "collector", "partition_gpu"),
	}
}

type PartitionGPUsCollector struct {
	gpuAlloc    *prometheus.Desc
	gpuTotal    *prometheus.Desc
	gpuIdle     *prometheus.Desc
	longestGRES int
	ignore      []string
	logger      log.Logger
}

// Send all metric descriptions
func (pc *PartitionGPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pc.gpuAlloc
	ch <- pc.gpuTotal
	ch <- pc.gpuIdle
}
func (pc *PartitionGPUsCollector) Collect(ch chan<- prometheus.Metric) {
	var timeout, errorMetric float64
	pm, err := PartitionGetGPUMetrics(pc.longestGRES, pc.ignore, pc.logger)
	if err == context.DeadlineExceeded {
		timeout = 1
	} else if err != nil {
		errorMetric = 1
	}
	for partition, count := range pm.gpuIdle {
		ch <- prometheus.MustNewConstMetric(pc.gpuIdle, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.gpuAlloc {
		ch <- prometheus.MustNewConstMetric(pc.gpuAlloc, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.gpuTotal {
		ch <- prometheus.MustNewConstMetric(pc.gpuTotal, prometheus.GaugeValue, count, partition)
	}
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errorMetric, "partition_gpu")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, timeout, "partition_gpu")
}
