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
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	ignoreNodeFeatures = kingpin.Flag("collector.node.ignore-features",
		"Regular expression of node features to ignore").Default("^$").String()
	nodeReasonLength = kingpin.Flag("collector.node.reason-length",
		"The length of reason string to append to down node metric, 0 to disable").Default("50").Int()
)

type NodesMetrics struct {
	alloc          float64
	comp           float64
	down           float64
	drain          float64
	err            float64
	fail           float64
	idle           float64
	maint          float64
	mix            float64
	resv           float64
	unknown        float64
	total          float64
	nodeState      map[string]string
	nodeDown       map[string]float64
	nodeDownReason map[string]string
	nodeFeatures   map[string]string
	allFeatures    []string
}

func NodesGetMetrics(logger log.Logger) (*NodesMetrics, error) {
	data, err := NodesData(logger)
	if err != nil {
		return &NodesMetrics{}, err
	}
	return ParseNodesMetrics(data), nil
}

func RemoveDuplicates(s []string) []string {
	m := map[string]bool{}
	t := []string{}

	// Walk through the slice 's' and for each value we haven't seen so far, append it to 't'.
	for _, v := range s {
		if _, seen := m[v]; !seen {
			t = append(t, v)
			m[v] = true
		}
	}

	return t
}

func ParseNodesMetrics(input string) *NodesMetrics {
	var nm NodesMetrics
	ignoreFeatures := regexp.MustCompile(*ignoreNodeFeatures)
	nodeState := make(map[string]string)
	nodeDown := make(map[string]float64)
	nodeDownReason := make(map[string]string)
	nodeFeatures := make(map[string]string)
	allFeatures := []string{}
	lines := strings.Split(input, "\n")

	// Sort and remove all the duplicates from the 'sinfo' output
	sort.Strings(lines)
	lines_uniq := RemoveDuplicates(lines)

	for _, line := range lines_uniq {
		if strings.Contains(line, "|") {
			items := strings.Split(line, "|")
			if len(items) != 4 {
				continue
			}
			node := items[0]
			state := items[1]
			features := strings.Split(strings.TrimSpace(items[2]), ",")
			reason := strings.TrimSpace(items[3])
			keepFeatures := []string{}
			nodeState[node] = strings.Trim(state, "*")
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
			nm.total++
			switch {
			case alloc.MatchString(state) == true:
				nm.alloc++
			case comp.MatchString(state) == true:
				nm.comp++
			case down.MatchString(state) == true:
				nm.down++
			case drain.MatchString(state) == true:
				nm.drain++
			case fail.MatchString(state) == true:
				nm.fail++
			case err.MatchString(state) == true:
				nm.err++
			case idle.MatchString(state) == true:
				nm.idle++
			case maint.MatchString(state) == true:
				nm.maint++
			case mix.MatchString(state) == true:
				nm.mix++
			case resv.MatchString(state) == true:
				nm.resv++
			case unknown.MatchString(state):
				nm.unknown++
			}
			if strings.HasSuffix(state, "*") || down.MatchString(state) || drain.MatchString(state) || fail.MatchString(state) {
				nodeDown[node] = 1
			} else {
				nodeDown[node] = 0
			}
			nodeDownReason[node] = reason
			for _, feature := range features {
				if !ignoreFeatures.MatchString(feature) {
					keepFeatures = append(keepFeatures, feature)
					if !sliceContains(allFeatures, feature) {
						allFeatures = append(allFeatures, feature)
					}
				}
			}
			nodeFeatures[node] = strings.Join(keepFeatures, ",")
		}
	}
	nm.nodeState = nodeState
	nm.nodeDown = nodeDown
	nm.nodeDownReason = nodeDownReason
	nm.nodeFeatures = nodeFeatures
	nm.allFeatures = allFeatures
	return &nm
}

// Execute the sinfo command and return its output
func NodesData(logger log.Logger) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*collectorTimeout)*time.Second)
	format := "%N|%T|%f|"
	defer cancel()
	if *nodeReasonLength != 0 {
		reasonFormat := fmt.Sprintf("%%%dE", *nodeReasonLength)
		format = format + reasonFormat
	}
	cmd := exec.CommandContext(ctx, "sinfo", "-a", "-h", "-N", "-o", format)
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

func NewNodesCollector(logger log.Logger) *NodesCollector {
	return &NodesCollector{
		alloc:        prometheus.NewDesc("slurm_nodes_alloc", "Allocated nodes", nil, nil),
		comp:         prometheus.NewDesc("slurm_nodes_comp", "Completing nodes", nil, nil),
		down:         prometheus.NewDesc("slurm_nodes_down", "Down nodes", nil, nil),
		drain:        prometheus.NewDesc("slurm_nodes_drain", "Drain nodes", nil, nil),
		err:          prometheus.NewDesc("slurm_nodes_err", "Error nodes", nil, nil),
		fail:         prometheus.NewDesc("slurm_nodes_fail", "Fail nodes", nil, nil),
		idle:         prometheus.NewDesc("slurm_nodes_idle", "Idle nodes", nil, nil),
		maint:        prometheus.NewDesc("slurm_nodes_maint", "Maint nodes", nil, nil),
		mix:          prometheus.NewDesc("slurm_nodes_mix", "Mix nodes", nil, nil),
		resv:         prometheus.NewDesc("slurm_nodes_resv", "Reserved nodes", nil, nil),
		unknown:      prometheus.NewDesc("slurm_nodes_unknown", "Unknown state nodes", nil, nil),
		total:        prometheus.NewDesc("slurm_nodes_total", "Total nodes", nil, nil),
		nodeState:    prometheus.NewDesc("slurm_node_state_info", "Node state", []string{"node", "state"}, nil),
		nodeDown:     prometheus.NewDesc("slurm_node_down", "Indicates if a node is down, 1=down 0=not down", []string{"node", "reason"}, nil),
		nodeFeatures: prometheus.NewDesc("slurm_node_features_info", "Node features", []string{"node", "features"}, nil),
		feature:      prometheus.NewDesc("slurm_node_feature", "Node feature", []string{"feature"}, nil),
		logger:       log.With(logger, "collector", "nodes"),
	}
}

type NodesCollector struct {
	alloc        *prometheus.Desc
	comp         *prometheus.Desc
	down         *prometheus.Desc
	drain        *prometheus.Desc
	err          *prometheus.Desc
	fail         *prometheus.Desc
	idle         *prometheus.Desc
	maint        *prometheus.Desc
	mix          *prometheus.Desc
	resv         *prometheus.Desc
	unknown      *prometheus.Desc
	total        *prometheus.Desc
	nodeState    *prometheus.Desc
	nodeDown     *prometheus.Desc
	nodeFeatures *prometheus.Desc
	feature      *prometheus.Desc
	logger       log.Logger
}

// Send all metric descriptions
func (nc *NodesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.alloc
	ch <- nc.comp
	ch <- nc.down
	ch <- nc.drain
	ch <- nc.err
	ch <- nc.fail
	ch <- nc.idle
	ch <- nc.maint
	ch <- nc.mix
	ch <- nc.resv
	ch <- nc.unknown
	ch <- nc.total
	ch <- nc.nodeState
	ch <- nc.nodeDown
	ch <- nc.nodeFeatures
	ch <- nc.feature
}
func (nc *NodesCollector) Collect(ch chan<- prometheus.Metric) {
	var timeout, errorMetric float64
	nm, err := NodesGetMetrics(nc.logger)
	if err == context.DeadlineExceeded {
		timeout = 1
	} else if err != nil {
		errorMetric = 1
	}
	ch <- prometheus.MustNewConstMetric(nc.alloc, prometheus.GaugeValue, nm.alloc)
	ch <- prometheus.MustNewConstMetric(nc.comp, prometheus.GaugeValue, nm.comp)
	ch <- prometheus.MustNewConstMetric(nc.down, prometheus.GaugeValue, nm.down)
	ch <- prometheus.MustNewConstMetric(nc.drain, prometheus.GaugeValue, nm.drain)
	ch <- prometheus.MustNewConstMetric(nc.err, prometheus.GaugeValue, nm.err)
	ch <- prometheus.MustNewConstMetric(nc.fail, prometheus.GaugeValue, nm.fail)
	ch <- prometheus.MustNewConstMetric(nc.idle, prometheus.GaugeValue, nm.idle)
	ch <- prometheus.MustNewConstMetric(nc.maint, prometheus.GaugeValue, nm.maint)
	ch <- prometheus.MustNewConstMetric(nc.mix, prometheus.GaugeValue, nm.mix)
	ch <- prometheus.MustNewConstMetric(nc.resv, prometheus.GaugeValue, nm.resv)
	ch <- prometheus.MustNewConstMetric(nc.unknown, prometheus.GaugeValue, nm.unknown)
	ch <- prometheus.MustNewConstMetric(nc.total, prometheus.GaugeValue, nm.total)
	for node, state := range nm.nodeState {
		ch <- prometheus.MustNewConstMetric(nc.nodeState, prometheus.GaugeValue, 1, node, state)
	}
	for node, down := range nm.nodeDown {
		var reason string
		if r, ok := nm.nodeDownReason[node]; ok {
			reason = r
		}
		ch <- prometheus.MustNewConstMetric(nc.nodeDown, prometheus.GaugeValue, down, node, reason)
	}
	for node, features := range nm.nodeFeatures {
		ch <- prometheus.MustNewConstMetric(nc.nodeFeatures, prometheus.GaugeValue, 1, node, features)
	}
	for _, feature := range nm.allFeatures {
		ch <- prometheus.MustNewConstMetric(nc.feature, prometheus.GaugeValue, 1, feature)
	}
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errorMetric, "nodes")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, timeout, "nodes")
}
