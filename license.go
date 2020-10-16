/* Copyright 2017 Victor Penso, Matteo Dessalvi

This code was adopted from cpus.go to capture license metrics using scontrol
Author: Trey Dockendorf <tdockendorf@osc.edu>

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

type LicenseMetrics struct {
	name  string
	total float64
	used  float64
	free  float64
}

func LicenseGetMetrics(logger log.Logger) ([]LicenseMetrics, error) {
	data, err := LicenseData(logger)
	if err != nil {
		return nil, err
	}
	return ParseLicenseMetrics(data, logger), nil
}

func ParseLicenseMetrics(input string, logger log.Logger) []LicenseMetrics {
	var lm []LicenseMetrics

	lines := strings.Split(input, "\n")
	for _, line := range lines {
		if !strings.HasPrefix(line, "LicenseName") {
			continue
		}
		m := LicenseMetrics{}
		fields := strings.Fields(line)
		for _, field := range fields {
			items := strings.Split(field, "=")
			if len(items) != 2 {
				continue
			}
			key := items[0]
			if key == "Remote" {
				continue
			}
			var value float64
			var err error
			if key != "LicenseName" {
				value, err = strconv.ParseFloat(items[1], 64)
				if err != nil {
					level.Error(logger).Log("msg", "Unable to parse license field value", "key", key, "value", items[1])
					continue
				}
			}
			switch key {
			case "LicenseName":
				m.name = items[1]
			case "Total":
				m.total = value
			case "Used":
				m.used = value
			case "Free":
				m.free = value
			}
		}
		if m.name != "" {
			lm = append(lm, m)
		}
	}

	return lm
}

func LicenseData(logger log.Logger) (string, error) {
	level.Debug(logger).Log("msg", "Query license data")
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*collectorTimeout)*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "scontrol", "show", "license", "-o")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			level.Error(logger).Log("msg", "Timeout executing scontrol")
			return "", ctx.Err()
		} else {
			level.Error(logger).Log("msg", "Error executing scontrol", "err", stderr.String(), "out", stdout.String())
			return "", err
		}
	}
	return stdout.String(), nil
}

func NewLicenseCollector(logger log.Logger) *LicenseCollector {
	return &LicenseCollector{
		total:  prometheus.NewDesc("slurm_license_total", "Total licenses", []string{"name"}, nil),
		used:   prometheus.NewDesc("slurm_license_used", "Used licenses", []string{"name"}, nil),
		free:   prometheus.NewDesc("slurm_license_free", "Free licenses", []string{"name"}, nil),
		logger: log.With(logger, "collector", "license"),
	}
}

type LicenseCollector struct {
	total  *prometheus.Desc
	used   *prometheus.Desc
	free   *prometheus.Desc
	logger log.Logger
}

func (cc *LicenseCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.total
	ch <- cc.used
	ch <- cc.free
}
func (cc *LicenseCollector) Collect(ch chan<- prometheus.Metric) {
	var timeout, errorMetric float64
	cm, err := LicenseGetMetrics(cc.logger)
	if err == context.DeadlineExceeded {
		timeout = 1
	} else if err != nil {
		errorMetric = 1
	}
	for _, m := range cm {
		ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, m.total, m.name)
		ch <- prometheus.MustNewConstMetric(cc.used, prometheus.GaugeValue, m.used, m.name)
		ch <- prometheus.MustNewConstMetric(cc.free, prometheus.GaugeValue, m.free, m.name)
	}
	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, errorMetric, "license")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, timeout, "license")
}
