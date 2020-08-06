/* Copyright 2017 Victor Penso, Matteo Dessalvi

This code was adopted from cpus.go to capture GPU metrics using slurm GRES
Author: Andrew E. Bruno <aebruno2@buffalo.edu>

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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var gpuGresPattern = regexp.MustCompile(`^gpu\:([^\:]+)\:?(\d+)?`)
var gpuGresLength = kingpin.Flag("collector.gpus.gres-length",
	"Length of GRES string to query").Default("100").Int()

type GPUsMetrics struct {
	alloc float64
	idle  float64
	total float64
}

func GPUsGetMetrics(logger log.Logger) (*GPUsMetrics, error) {
	data, err := GPUsData(logger)
	if err != nil {
		return &GPUsMetrics{}, err
	}
	return ParseGPUsMetrics(data), nil
}

func parseGRES(line string) float64 {
	value := 0.0

	gres := strings.Split(line, ",")
	for _, g := range gres {
		if !strings.HasPrefix(g, "gpu:") {
			continue
		}

		matches := gpuGresPattern.FindStringSubmatch(g)
		if len(matches) == 3 {
			if matches[2] != "" {
				value, _ = strconv.ParseFloat(matches[2], 64)
			} else {
				value, _ = strconv.ParseFloat(matches[1], 64)
			}
		}
	}

	return value
}

func ParseGPUsMetrics(input string) *GPUsMetrics {
	var gm GPUsMetrics
	seen := make(map[string]bool)

	lines := strings.Split(input, "\n")
	for _, line := range lines {
		data := strings.Fields(line)
		if len(data) != 3 {
			continue
		}

		if _, ok := seen[data[0]]; ok {
			continue
		}

		seen[data[0]] = true

		// This is how many GPUs are available on the node
		avail := parseGRES(data[1])

		// This is how many GPUs are allocated on the node
		alloc := parseGRES(data[2])

		gm.alloc += alloc
		gm.total += avail
		gm.idle += (avail - alloc)
	}

	return &gm
}

// Execute the sinfo command to list all nodes and their associated GRES
// information. Note: nodes can be in more than one partition so need to dedup
// the output of sinof by nodehost.
func GPUsData(logger log.Logger) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*collectorTimeout)*time.Second)
	defer cancel()
	format := fmt.Sprintf("--Format=nodehost,gres:%d,gresused:%d", *gpuGresLength, *gpuGresLength)
	cmd := exec.CommandContext(ctx, "sinfo", "-a", "-h", "--Node", format)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			level.Error(logger).Log("msg", "Timeout executing sinfo")
			return "", ctx.Err()
		} else {
			level.Error(logger).Log("msg", "Error executing sinfo", "err", stderr.String(), "out", stdout.String())
			return "", err
		}
	}
	return stdout.String(), nil
}

func NewGPUsCollector(logger log.Logger) *GPUsCollector {
	return &GPUsCollector{
		alloc:  prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs", nil, nil),
		idle:   prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs", nil, nil),
		total:  prometheus.NewDesc("slurm_gpus_total", "Total GPUs", nil, nil),
		logger: log.With(logger, "collector", "gpus"),
	}
}

type GPUsCollector struct {
	alloc  *prometheus.Desc
	idle   *prometheus.Desc
	total  *prometheus.Desc
	logger log.Logger
}

func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.total
}
func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	var timeout, errorMetric float64
	cm, err := GPUsGetMetrics(cc.logger)
	if err == context.DeadlineExceeded {
		timeout = 1
	} else if err != nil {
		errorMetric = 1
	}
	ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, cm.alloc)
	ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, cm.idle)
	ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, cm.total)
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errorMetric, "gpus")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, timeout, "gpus")
}
