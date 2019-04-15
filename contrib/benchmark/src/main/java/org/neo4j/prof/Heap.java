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
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.Defaults;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.util.Collection;
import java.util.Collections;

public final class Heap implements InternalProfiler {

    private long before;

    @Override
    public void beforeIteration(final BenchmarkParams benchmarkParams, final IterationParams iterationParams) {
        before = usedHeap();
    }

    @Override
    public Collection<? extends Result> afterIteration(
            final BenchmarkParams benchmarkParams,
            final IterationParams iterationParams,
            final IterationResult result) {
        long allUsage = usedHeap() - before;
        long ops = result.getMetadata().getAllOps();
        double perOpUsage = (double) allUsage / ops;

        return Collections.singletonList(
                new ScalarResult(Defaults.PREFIX + "heap.alloc.norm", perOpUsage, "B/op", AggregationPolicy.AVG)
        );
    }

    @Override
    public String getDescription() {
        return "Simple, naive Heap consumption during benchmark. For more detailed profiling, use gc profiler.";
    }

    private static long usedHeap() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
