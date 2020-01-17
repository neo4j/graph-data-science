/*
 * Copyright (c) 2017-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.k1coloring.ColoringStep;
import org.neo4j.graphalgo.k1coloring.K1Coloring;
import org.neo4j.graphdb.Direction;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.neo4j.graphalgo.core.utils.ParallelUtil.DEFAULT_BATCH_SIZE;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms4g", "-Xmx4g"})
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class K1ColoringBenchmark {

    private Graph graph;
    private BitSet nodesToColor;
    private HugeLongArray colors;

    @Setup
    public void setup() {
        graph = new RandomGraphGenerator(1_000_000, 50, RelationshipDistribution.POWER_LAW, Optional.empty(), AllocationTracker.EMPTY).generate();
    }

    @TearDown
    public void tearDown() {
        graph.release();
        Pools.DEFAULT.shutdownNow();
    }

    @Setup(Level.Invocation)
    public void foo() {
        colors = HugeLongArray.newArray(graph.nodeCount(), AllocationTracker.EMPTY);
        colors.setAll((i) -> 1);

        nodesToColor = new BitSet(graph.nodeCount());
        nodesToColor.set(0, graph.nodeCount());
    }

    @Benchmark
    public void singleIteration(Blackhole blackhole) {
        K1Coloring k1Coloring = initAlgo(1);
        k1Coloring.compute();
        blackhole.consume(k1Coloring.colors());
    }

    @Benchmark
    public void tenIterations(Blackhole blackhole) {
        K1Coloring k1Coloring = initAlgo(10);
        k1Coloring.compute();
        blackhole.consume(k1Coloring.colors());
    }

    @Benchmark
    public void coloringStep(Blackhole blackhole) {
        ColoringStep coloringStep = new ColoringStep(
            graph,
            Direction.BOTH,
            colors,
            nodesToColor,
            graph.nodeCount(),
            0,
            graph.nodeCount()
        );
        coloringStep.run();
        blackhole.consume(colors);
    }

    private K1Coloring initAlgo(int maxIterations) {
        return new K1Coloring(
            graph,
            Direction.BOTH,
            maxIterations,
            DEFAULT_BATCH_SIZE,
            4,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );
    }
}