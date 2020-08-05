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
	"net/http"
	"os"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	namespace = "slurm"
)

func metricsHandler(logger log.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		registry := prometheus.NewRegistry()
		// Metrics have to be registered to be exposed
		registry.MustRegister(NewSchedulerCollector(logger)) // from scheduler.go
		if *slurmdbEnable {
			registry.MustRegister(NewSlurmdbdCollector(logger)) // from slurmdbd.go
		}
		registry.MustRegister(NewQueueCollector(logger)) // from queue.go
		registry.MustRegister(NewNodesCollector(logger)) // from nodes.go
		registry.MustRegister(NewCPUsCollector(logger))  // from cpus.go
		registry.MustRegister(NewGPUsCollector(logger))  // from gpus.go
		if *partitionEnable {
			registry.MustRegister(NewPartitionCollector(logger)) // from partition.go
		}

		gatherers := prometheus.Gatherers{registry, prometheus.DefaultGatherer}
		h := promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{})
		h.ServeHTTP(w, r)
	}
}

var (
	listenAddress = kingpin.Flag("listen-address",
		"Address to listen on for web interface and telemetry.").Default(":8080").String()
	collectorTimeout = kingpin.Flag("collector-timeout",
		"Time in seconds each collector can run before being timed out").Default("30").Int()
	collectError = prometheus.NewDesc("slurm_exporter_collect_error",
		"Indicates if an error has occurred during collection", []string{"collector"}, nil)
	collecTimeout = prometheus.NewDesc("slurm_exporter_collect_timeout",
		"Indicates the collector timed out", []string{"collector"}, nil)
)

func sliceContains(slice []string, str string) bool {
	for _, s := range slice {
		if str == s {
			return true
		}
	}
	return false
}

func main() {
	promlogConfig := &promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.Version(version.Print("gpfs_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	logger := promlog.New(promlogConfig)
	level.Info(logger).Log("msg", "Starting slurm exporter", "version", version.Info())
	level.Info(logger).Log("msg", "Build context", "build_context", version.BuildContext())
	level.Info(logger).Log("msg", "Starting Server", "address", *listenAddress)

	// The Handler function provides a default handler to expose metrics
	// via an HTTP server. "/metrics" is the usual endpoint for that.
	http.Handle("/metrics", metricsHandler(logger))
	err := http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
}
