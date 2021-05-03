/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.graphalgo.beta.k1coloring;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig.AllowSelfLoops;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.UnionGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.progress.EmptyProgressEventTracker;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestSupport.fromGdl;
import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.DEFAULT_BATCH_SIZE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class K1ColoringTest {

    @Test
    void testK1Coloring() {
        final String DB_CYPHER =
            "CREATE" +
            " (a)" +
            ",(b)" +
            ",(c)" +
            ",(d)" +
            ",(a)-[:REL]->(b)" +
            ",(a)-[:REL]->(c)";

        var graph = fromGdl(DB_CYPHER);

        K1Coloring k1Coloring = new K1Coloring(
            graph,
            1000,
            DEFAULT_BATCH_SIZE,
            1,
            Pools.DEFAULT,
            ProgressLogger.NULL_LOGGER,
            AllocationTracker.empty()
        );

        k1Coloring.compute();

        HugeLongArray colors = k1Coloring.colors();

        assertNotEquals(colors.get(0), colors.get(1));
        assertNotEquals(colors.get(0), colors.get(2));
        assertEquals(colors.get(1), colors.get(2));
    }

    @Test
    void testParallelK1Coloring() {
        long seed = 42L;

        RandomGraphGenerator outGenerator = RandomGraphGenerator.builder()
            .nodeCount(100_000)
            .averageDegree(5)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .seed(seed)
            .allocationTracker(AllocationTracker.empty())
            .build();

        RandomGraphGenerator inGenerator = RandomGraphGenerator.builder()
            .nodeCount(100_000)
            .averageDegree(5)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .seed(seed)
            .aggregation(Aggregation.NONE)
            .orientation(Orientation.REVERSE)
            .allowSelfLoops(AllowSelfLoops.NO)
            .allocationTracker(AllocationTracker.empty())
            .build();

        var naturalGraph = outGenerator.generate();
        var reverseGraph = inGenerator.generate();
        var graph = UnionGraph.of(Arrays.asList(naturalGraph, reverseGraph));

        K1Coloring k1Coloring = new K1Coloring(
            graph,
            100,
            DEFAULT_BATCH_SIZE,
            8,
            Pools.DEFAULT,
            ProgressLogger.NULL_LOGGER,
            AllocationTracker.empty()
        );

        k1Coloring.compute();
        HugeLongArray colors = k1Coloring.colors();

        Set<Long> colorsUsed = new HashSet<>(100);
        MutableLong conflicts = new MutableLong(0);
        graph.forEachNode((nodeId) -> {
            graph.forEachRelationship(nodeId, (source, target) -> {
                if (source != target && colors.get(source) == colors.get(target)) {
                    conflicts.increment();
                }
                colorsUsed.add(colors.get(source));
                return true;
            });
            return true;
        });

        assertTrue(conflicts.getValue() < 20);
        assertTrue(colorsUsed.size() < 20);
    }


    @Test
    void shouldComputeMemoryEstimation1Thread() {
        long nodeCount = 100_000L;
        int concurrency = 1;

        assertMemoryEstimation(nodeCount, concurrency, 825248);
    }

    @Test
    void shouldComputeMemoryEstimation4Threads() {
        long nodeCount = 100_000L;
        int concurrency = 4;
        assertMemoryEstimation(nodeCount, concurrency, 863000);
    }

    @Test
    void shouldComputeMemoryEstimation42Threads() {
        long nodeCount = 100_000L;
        int concurrency = 42;
        assertMemoryEstimation(nodeCount, concurrency, 1341192);
    }

    @Test
    void everyNodeShouldHaveBeenColored() {
        RandomGraphGenerator generator = RandomGraphGenerator.builder()
            .nodeCount(100_000)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .allocationTracker(AllocationTracker.empty())
            .build();

        Graph graph = generator.generate();

        K1Coloring k1Coloring = new K1Coloring(
            graph,
            100,
            DEFAULT_BATCH_SIZE,
            8,
            Pools.DEFAULT,
            ProgressLogger.NULL_LOGGER,
            AllocationTracker.empty()
        );

        k1Coloring.compute();

        assertFalse(k1Coloring.usedColors().get(ColoringStep.INITIAL_FORBIDDEN_COLORS));
    }

    @Test
    void shouldLogProgress(){
        var graph = RandomGraphGenerator.builder()
            .nodeCount(100)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .allocationTracker(AllocationTracker.empty())
            .seed(42L)
            .build()
            .generate();

        var testLogger = new TestProgressLogger(graph.relationshipCount() * 2, "K1Coloring", 8, EmptyProgressEventTracker.INSTANCE);

        var k1Coloring = new K1Coloring(
            graph,
            100,
            DEFAULT_BATCH_SIZE,
            8,
            Pools.DEFAULT,
            testLogger,
            AllocationTracker.empty()
        );

        k1Coloring.compute();

        List<AtomicLong> progresses = testLogger.getProgresses();

        assertEquals(k1Coloring.ranIterations(), progresses.size());
        progresses.forEach(progress -> assertTrue(progress.get() <= 2 * graph.relationshipCount()));

        assertTrue(testLogger.containsMessage(TestLog.INFO, ":: Start"));
        LongStream.range(1, k1Coloring.ranIterations() + 1).forEach(iteration -> {
            assertTrue(testLogger.containsMessage(TestLog.INFO, formatWithLocale("Iteration %d :: Start", iteration)));
            assertTrue(testLogger.containsMessage(TestLog.INFO, formatWithLocale("Iteration %d :: Start", iteration)));
        });
        assertTrue(testLogger.containsMessage(TestLog.INFO, ":: Finished"));
    }

    private void assertMemoryEstimation(long nodeCount, int concurrency, long expected) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();
        K1ColoringStreamConfig config = ImmutableK1ColoringStreamConfig.builder().build();
        final MemoryRange actual = new K1ColoringFactory<>()
            .memoryEstimation(config)
            .estimate(dimensions, concurrency)
            .memoryUsage();

        assertEquals(actual.min, actual.max);
        assertEquals(expected, actual.min);
    }

}



