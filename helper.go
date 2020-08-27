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
	"os/exec"
    "strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

func LongestGRES(logger log.Logger) int {
	longest := 50
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*collectorTimeout)*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sinfo", "-o", "%G")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			level.Error(logger).Log("msg", "Timeout executing sinfo")
			return longest
		} else {
			level.Error(logger).Log("msg", "Error executing sinfo", "err", stderr.String(), "out", stdout.String())
			return longest
		}
	}
	lines := strings.Split(stdout.String(), "\n")
	for _, line := range lines {
		size := len(line)
		if size > longest {
			longest = size
		}
	}
	return longest + 20
}
