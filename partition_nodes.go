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
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

type PartitionNodeMetrics struct {
	nodeAlloc   map[string]float64
	nodeComp    map[string]float64
	nodeDown    map[string]float64
	nodeDrain   map[string]float64
	nodeErr     map[string]float64
	nodeFail    map[string]float64
	nodeIdle    map[string]float64
	nodeMaint   map[string]float64
	nodeMix     map[string]float64
	nodeResv    map[string]float64
	nodeUnknown map[string]float64
}

func PartitionGetNodeMetrics(partitions []string, ignore []string, logger log.Logger) (*PartitionNodeMetrics, error) {
	nodeData, err := PartitionNodesData(logger)
	if err != nil {
		return &PartitionNodeMetrics{}, err
	}
	return ParsePartitionNodeMetrics(nodeData, partitions, ignore), nil
}

func ParsePartitionNodeMetrics(nodeData string, partitions []string, ignore []string) *PartitionNodeMetrics {
	var pm PartitionNodeMetrics

	ParsePartitionNodes(nodeData, &pm, ignore)

	for _, part := range partitions {
		if sliceContains(ignore, part) {
			continue
		}
		if _, ok := pm.nodeAlloc[part]; !ok {
			pm.nodeAlloc[part] = 0
		}
		if _, ok := pm.nodeComp[part]; !ok {
			pm.nodeComp[part] = 0
		}
		if _, ok := pm.nodeDown[part]; !ok {
			pm.nodeDown[part] = 0
		}
		if _, ok := pm.nodeDrain[part]; !ok {
			pm.nodeDrain[part] = 0
		}
		if _, ok := pm.nodeErr[part]; !ok {
			pm.nodeErr[part] = 0
		}
		if _, ok := pm.nodeFail[part]; !ok {
			pm.nodeFail[part] = 0
		}
		if _, ok := pm.nodeIdle[part]; !ok {
			pm.nodeIdle[part] = 0
		}
		if _, ok := pm.nodeMaint[part]; !ok {
			pm.nodeMaint[part] = 0
		}
		if _, ok := pm.nodeMix[part]; !ok {
			pm.nodeMix[part] = 0
		}
		if _, ok := pm.nodeResv[part]; !ok {
			pm.nodeResv[part] = 0
		}
		if _, ok := pm.nodeUnknown[part]; !ok {
			pm.nodeUnknown[part] = 0
		}
	}

	return &pm
}

func ParsePartitionNodes(input string, pm *PartitionNodeMetrics, ignore []string) {
	nodeAlloc := make(map[string]float64)
	nodeComp := make(map[string]float64)
	nodeDown := make(map[string]float64)
	nodeDrain := make(map[string]float64)
	nodeErr := make(map[string]float64)
	nodeFail := make(map[string]float64)
	nodeIdle := make(map[string]float64)
	nodeMaint := make(map[string]float64)
	nodeMix := make(map[string]float64)
	nodeResv := make(map[string]float64)
	nodeUnknown := make(map[string]float64)

	nodelines := strings.Split(input, "\n")
	for _, line := range nodelines {
		items := strings.Split(line, ",")
		if len(items) != 2 {
			continue
		}
		part := items[0]
		state := items[1]
		if sliceContains(ignore, part) {
			continue
		}
		alloc := regexp.MustCompile(`^alloc`)
		comp := regexp.MustCompile(`^comp`)
		down := regexp.MustCompile(`^down`)
		drain := regexp.MustCompile(`^drain`)
		fail := regexp.MustCompile(`^fail`)
		err := regexp.MustCompile(`^err`)
		idle := regexp.MustCompile(`^idle`)
		maint := regexp.MustCompile(`^maint`)
		mix := regexp.MustCompile(`^mix`)
		resv := regexp.MustCompile(`^res`)
		unknown := regexp.MustCompile(`^unknown`)
		switch {
		case alloc.MatchString(state):
			nodeAlloc[part]++
		case comp.MatchString(state):
			nodeComp[part]++
		case down.MatchString(state):
			nodeDown[part]++
		case drain.MatchString(state):
			nodeDrain[part]++
		case fail.MatchString(state):
			nodeFail[part]++
		case err.MatchString(state):
			nodeErr[part]++
		case idle.MatchString(state):
			nodeIdle[part]++
		case maint.MatchString(state):
			nodeMaint[part]++
		case mix.MatchString(state):
			nodeMix[part]++
		case resv.MatchString(state):
			nodeResv[part]++
		case unknown.MatchString(state):
			nodeUnknown[part]++
		}
	}
	pm.nodeAlloc = nodeAlloc
	pm.nodeComp = nodeComp
	pm.nodeDown = nodeDown
	pm.nodeDrain = nodeDrain
	pm.nodeErr = nodeErr
	pm.nodeFail = nodeFail
	pm.nodeIdle = nodeIdle
	pm.nodeMaint = nodeMaint
	pm.nodeMix = nodeMix
	pm.nodeResv = nodeResv
	pm.nodeUnknown = nodeUnknown
}

// Execute the sinfo command and return its output
func PartitionNodesData(logger log.Logger) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*collectorTimeout)*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sinfo", "-a", "-h", "-N", "-o", "%R,%T")
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

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewPartitionNodesCollector(partitions []string, ignore []string, logger log.Logger) *PartitionNodesCollector {
	labels := []string{"partition"}
	return &PartitionNodesCollector{
		nodeAlloc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "nodes_alloc"),
			"Allocated nodes", labels, nil),
		nodeComp: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "nodes_comp"),
			"Completing nodes", labels, nil),
		nodeDown: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "nodes_down"),
			"Down nodes", labels, nil),
		nodeDrain: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "nodes_drain"),
			"Drain nodes", labels, nil),
		nodeErr: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "nodes_err"),
			"Error nodes", labels, nil),
		nodeFail: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "nodes_fail"),
			"Fail nodes", labels, nil),
		nodeIdle: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "nodes_idle"),
			"Idle nodes", labels, nil),
		nodeMaint: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "nodes_maint"),
			"Maint nodes", labels, nil),
		nodeMix: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "nodes_mix"),
			"Mix nodes", labels, nil),
		nodeResv: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "nodes_resv"),
			"Reserved nodes", labels, nil),
		nodeUnknown: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "partition", "nodes_unknown"),
			"Unknown state nodes", labels, nil),
		partitions: partitions,
		ignore:     ignore,
		logger:     log.With(logger, "collector", "partition_node"),
	}
}

type PartitionNodesCollector struct {
	nodeAlloc   *prometheus.Desc
	nodeComp    *prometheus.Desc
	nodeDown    *prometheus.Desc
	nodeDrain   *prometheus.Desc
	nodeErr     *prometheus.Desc
	nodeFail    *prometheus.Desc
	nodeIdle    *prometheus.Desc
	nodeMaint   *prometheus.Desc
	nodeMix     *prometheus.Desc
	nodeResv    *prometheus.Desc
	nodeUnknown *prometheus.Desc
	partitions  []string
	ignore      []string
	logger      log.Logger
}

// Send all metric descriptions
func (pc *PartitionNodesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pc.nodeAlloc
	ch <- pc.nodeComp
	ch <- pc.nodeDown
	ch <- pc.nodeDrain
	ch <- pc.nodeErr
	ch <- pc.nodeFail
	ch <- pc.nodeIdle
	ch <- pc.nodeMaint
	ch <- pc.nodeMix
	ch <- pc.nodeResv
	ch <- pc.nodeUnknown
}
func (pc *PartitionNodesCollector) Collect(ch chan<- prometheus.Metric) {
	var timeout, errorMetric float64
	pm, err := PartitionGetNodeMetrics(pc.partitions, pc.ignore, pc.logger)
	if err == context.DeadlineExceeded {
		timeout = 1
	} else if err != nil {
		errorMetric = 1
	}
	for partition, count := range pm.nodeAlloc {
		ch <- prometheus.MustNewConstMetric(pc.nodeAlloc, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.nodeComp {
		ch <- prometheus.MustNewConstMetric(pc.nodeComp, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.nodeDown {
		ch <- prometheus.MustNewConstMetric(pc.nodeDown, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.nodeDrain {
		ch <- prometheus.MustNewConstMetric(pc.nodeDrain, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.nodeErr {
		ch <- prometheus.MustNewConstMetric(pc.nodeErr, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.nodeFail {
		ch <- prometheus.MustNewConstMetric(pc.nodeFail, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.nodeIdle {
		ch <- prometheus.MustNewConstMetric(pc.nodeIdle, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.nodeMaint {
		ch <- prometheus.MustNewConstMetric(pc.nodeMaint, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.nodeMix {
		ch <- prometheus.MustNewConstMetric(pc.nodeMix, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.nodeResv {
		ch <- prometheus.MustNewConstMetric(pc.nodeResv, prometheus.GaugeValue, count, partition)
	}
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errorMetric, "partition_node")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, timeout, "partition_node")
}
