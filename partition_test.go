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
	"io/ioutil"
	"os"
	"testing"

	"github.com/go-kit/kit/log"
	"gopkg.in/alecthomas/kingpin.v2"
)

func TestPartitionMetrics(t *testing.T) {
	// Read the input data from a file
	cpuFile, cpuErr := os.Open("test_data/sinfo_partition_cpus.txt")
	if cpuErr != nil {
		t.Fatalf("Can not open test data: %v", cpuErr)
	}
	nodeFile, nodeErr := os.Open("test_data/sinfo_partition_nodestates.txt")
	if nodeErr != nil {
		t.Fatalf("Can not open test data :%v", nodeErr)
	}
	jobFile, jobErr := os.Open("test_data/squeue_partitions.txt")
	if jobErr != nil {
		t.Fatalf("Can not open test data :%v", jobErr)
	}
	gpuFile, gpuErr := os.Open("test_data/sinfo_partition_gpus.txt")
	if gpuErr != nil {
		t.Fatalf("Can not open test data :%v", gpuErr)
	}
	cpuData, _ := ioutil.ReadAll(cpuFile)
	nodeData, _ := ioutil.ReadAll(nodeFile)
	jobData, _ := ioutil.ReadAll(jobFile)
	gpuData, _ := ioutil.ReadAll(gpuFile)
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)
	t.Logf("%+v", ParsePartitionMetrics(string(cpuData), string(nodeData), string(jobData), string(gpuData), logger))
}

func TestPartitionGetMetrics(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	timeout := 10
	collectorTimeout = &timeout
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)
	data, _ := PartitionGetMetrics(logger)
	t.Logf("%+v", data)
}
