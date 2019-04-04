/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.prof;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.profile.ExternalProfiler;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.Result;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Runs Java Flight Recorder during JMH benchmarking.
 * <p>
 * To use: add {@code -prof org.neo4j.prof.JFR} to your JHM call.
 * <p>
 * The profiler unlock commercial JVM features.
 * It delays the recording for the amount of time that the warmup phase likely takes.
 * If your warmups don't run as estimated, the recording might start late or early.
 * <p>
 * Recordings are saved in a file per benchmark, the filename is printed at the end of each benchmark.
 */
public final class JFR implements ExternalProfiler {
    @Override
    public Collection<String> addJVMInvokeOptions(final BenchmarkParams params) {
        return Collections.emptySet();
    }

    @Override
    public Collection<String> addJVMOptions(final BenchmarkParams params) {
        final String fileName = getFileName(params);
        return Arrays.asList(
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+DebugNonSafepoints",
                "-XX:+UnlockCommercialFeatures",
                "-XX:+FlightRecorder",
                "-XX:StartFlightRecording=duration=0s,delay=0s,dumponexit=true,filename=" + fileName
        );
    }

    @Override
    public void beforeTrial(final BenchmarkParams benchmarkParams) {
        // do nothing
    }

    @Override
    public Collection<? extends Result> afterTrial(
            final BenchmarkResult br,
            final long pid,
            final File stdOut,
            final File stdErr) {
        final String fileName = getFileName(br.getParams());
        return Collections.singleton(
                new NoResult("JFR", "Results save to " + fileName));
    }

    @Override
    public boolean allowPrintOut() {
        return true;
    }

    @Override
    public boolean allowPrintErr() {
        return true;
    }

    @Override
    public String getDescription() {
        return "runs the flight recorder alongside the benchmark";
    }

    private String getFileName(final BenchmarkParams params) {
        return params.id().replaceAll("['\"]", "-") + ".jfr";
    }
}
