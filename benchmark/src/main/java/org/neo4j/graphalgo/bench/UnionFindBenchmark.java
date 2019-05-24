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
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * @author mknblch
 */
@Threads(1)
@Fork(1)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class UnionFindBenchmark {

    @Param({"HEAVY_QUEUE", "HEAVY_FORK_JOIN", "HEAVY_FJ_MERGE", "HEAVY_SEQ", "HUGE_QUEUE", "HUGE_FORK_JOIN", "HUGE_FJ_MERGE", "HUGE_SEQ", "HUGE_HUGE_QUEUE", "HUGE_HUGE_FORK_JOIN", "HUGE_HUGE_FJ_MERGE", "HUGE_HUGE_SEQ"})
    UFBenchmarkCombination uf;

    private Graph theGraph;

    @Setup
    public void setup(HeroGraph heroGraph) {
        theGraph = new GraphLoader(heroGraph.db).load(uf.graph.impl);
        heroGraph.db.shutdown();
    }

    @TearDown
    public void tearDown() {
        theGraph.release();
        Pools.DEFAULT.shutdownNow();
    }


    @Benchmark
    public Object unionFind() {
        return uf.run(theGraph);
    }
}
