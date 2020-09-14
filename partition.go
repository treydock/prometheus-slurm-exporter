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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/montanaflynn/stats"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	timeNowFunc     = time.Now
	timeFormat      = "2006-01-02T15:04:05 MST"
	partitionEnable = kingpin.Flag("collector.partition",
		"Enable the partition collector").Default("false").Bool()
	ignorePartitions = kingpin.Flag("collector.partition.ignore",
		"Comma separated list of partitions to ignore").Default("").String()
)

type PartitionMetrics struct {
	nodeAlloc       map[string]float64
	nodeComp        map[string]float64
	nodeDown        map[string]float64
	nodeDrain       map[string]float64
	nodeErr         map[string]float64
	nodeFail        map[string]float64
	nodeIdle        map[string]float64
	nodeMaint       map[string]float64
	nodeMix         map[string]float64
	nodeResv        map[string]float64
	nodeUnknown     map[string]float64
	cpuAlloc        map[string]float64
	cpuIdle         map[string]float64
	cpuOther        map[string]float64
	cpuTotal        map[string]float64
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
	gpuAlloc        map[string]float64
	gpuTotal        map[string]float64
	gpuIdle         map[string]float64
}

func PartitionGetMetrics(longestGRES int, logger log.Logger) (*PartitionMetrics, error) {
	var cpuData, nodeData, jobData, gpuData string
	var cpuErr, nodeErr, jobErr, gpuErr error

	wg := &sync.WaitGroup{}
	wg.Add(4)

	go func() {
		cpuData, cpuErr = PartitionCPUsData(logger)
		wg.Done()
	}()
	go func() {
		nodeData, nodeErr = PartitionNodesData(logger)
		wg.Done()
	}()
	go func() {
		jobData, jobErr = PartitionJobData(logger)
		wg.Done()
	}()
	go func() {
		gpuData, gpuErr = PartitionGPUsData(longestGRES, logger)
		wg.Done()
	}()
	wg.Wait()
	if cpuErr != nil {
		return &PartitionMetrics{}, cpuErr
	}
	if nodeErr != nil {
		return &PartitionMetrics{}, nodeErr
	}
	if jobErr != nil {
		return &PartitionMetrics{}, jobErr
	}
	if gpuErr != nil {
		return &PartitionMetrics{}, gpuErr
	}
	return ParsePartitionMetrics(cpuData, nodeData, jobData, gpuData, logger), nil
}

func ParsePartitionMetrics(cpuData string, nodeData string, jobData string, gpuData string, logger log.Logger) *PartitionMetrics {
	ignoreParts := strings.Split(*ignorePartitions, ",")
	var pm PartitionMetrics

	partitions := ParsePartitionCPUs(cpuData, &pm, ignoreParts)
	ParsePartitionNodes(nodeData, &pm, partitions)
	ParsePartitionJobs(jobData, &pm, partitions, logger)
	ParsePartitionGPUs(gpuData, &pm, partitions)

	for _, part := range partitions {
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

func ParsePartitionCPUs(input string, pm *PartitionMetrics, ignore []string) []string {
	partitions := []string{}
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
		if !sliceContains(partitions, partition) {
			partitions = append(partitions, partition)
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
	return partitions
}

func ParsePartitionNodes(input string, pm *PartitionMetrics, partitions []string) {
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
		if !sliceContains(partitions, part) {
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

func ParsePartitionJobs(input string, pm *PartitionMetrics, partitions []string, logger log.Logger) {
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
		if !sliceContains(partitions, partition) {
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

func ParsePartitionGPUs(input string, pm *PartitionMetrics, partitions []string) {
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
		if !sliceContains(partitions, part) {
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

func NewPartitionCollector(longestGRES int, logger log.Logger) *PartitionCollector {
	labels := []string{"partition"}
	waitLabels := []string{"partition", "reason"}
	return &PartitionCollector{
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
		logger:      log.With(logger, "collector", "partition"),
	}
}

type PartitionCollector struct {
	cpuAlloc        *prometheus.Desc
	cpuIdle         *prometheus.Desc
	cpuOther        *prometheus.Desc
	cpuTotal        *prometheus.Desc
	nodeAlloc       *prometheus.Desc
	nodeComp        *prometheus.Desc
	nodeDown        *prometheus.Desc
	nodeDrain       *prometheus.Desc
	nodeErr         *prometheus.Desc
	nodeFail        *prometheus.Desc
	nodeIdle        *prometheus.Desc
	nodeMaint       *prometheus.Desc
	nodeMix         *prometheus.Desc
	nodeResv        *prometheus.Desc
	nodeUnknown     *prometheus.Desc
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
	gpuAlloc        *prometheus.Desc
	gpuTotal        *prometheus.Desc
	gpuIdle         *prometheus.Desc
	longestGRES     int
	logger          log.Logger
}

// Send all metric descriptions
func (pc *PartitionCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pc.cpuAlloc
	ch <- pc.cpuIdle
	ch <- pc.cpuOther
	ch <- pc.cpuTotal
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
	ch <- pc.gpuAlloc
	ch <- pc.gpuTotal
	ch <- pc.gpuIdle
}
func (pc *PartitionCollector) Collect(ch chan<- prometheus.Metric) {
	var timeout, errorMetric float64
	pm, err := PartitionGetMetrics(pc.longestGRES, pc.logger)
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
	for partition, count := range pm.gpuIdle {
		ch <- prometheus.MustNewConstMetric(pc.gpuIdle, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.gpuAlloc {
		ch <- prometheus.MustNewConstMetric(pc.gpuAlloc, prometheus.GaugeValue, count, partition)
	}
	for partition, count := range pm.gpuTotal {
		ch <- prometheus.MustNewConstMetric(pc.gpuTotal, prometheus.GaugeValue, count, partition)
	}
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errorMetric, "partition")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, timeout, "partition")
}
