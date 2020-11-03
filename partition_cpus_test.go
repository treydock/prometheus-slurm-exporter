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

func TestParsePartitionCPUs(t *testing.T) {
	// Read the input data from a file
	cpuFile, err := os.Open("test_data/sinfo_partition_cpus.txt")
	if err != nil {
		t.Fatalf("Can not open test data: %v", err)
	}
	cpuData, _ := ioutil.ReadAll(cpuFile)
	t.Logf("%+v", ParsePartitionCPUs(string(cpuData), []string{}))
}

func TestPartitionGetCPUMetrics(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	timeout := 10
	collectorTimeout = &timeout
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)
	data, _ := PartitionGetCPUMetrics([]string{}, logger)
	t.Logf("%+v", data)
}
