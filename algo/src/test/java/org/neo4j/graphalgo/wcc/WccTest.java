/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.graphalgo.wcc;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

class WccTest {

    private static final int SETS_COUNT = 16;
    private static final int SET_SIZE = 10;

    /**
     * Compute number of sets present.
     */
    static long getSetCount(DisjointSetStruct struct) {
        long capacity = struct.size();
        BitSet sets = new BitSet(capacity);
        for (long i = 0L; i < capacity; i++) {
            long setId = struct.setIdOf(i);
            sets.set(setId);
        }
        return sets.cardinality();
    }

    int communitySize() {
        return SET_SIZE;
    }

    @ParameterizedTest(name = "orientation = {0}")
    @EnumSource(Orientation.class)
    void shouldComputeComponents(Orientation orientation) {
        var graph = createTestGraph(orientation);

        DisjointSetStruct result = run(graph);

        assertEquals(SETS_COUNT, getSetCount(result));
        long[] setRegions = new long[SETS_COUNT];
        Arrays.fill(setRegions, -1);

        graph.forEachNode((nodeId) -> {
            long expectedSetRegion = nodeId / SET_SIZE;
            final long setId = result.setIdOf(nodeId);
            int setRegion = (int) (setId / SET_SIZE);
            assertEquals(
                expectedSetRegion,
                setRegion,
                "Node " + nodeId + " in unexpected set: " + setId
            );

            long regionSetId = setRegions[setRegion];
            if (regionSetId == -1) {
                setRegions[setRegion] = setId;
            } else {
                assertEquals(
                    regionSetId,
                    setId,
                    "Inconsistent set for node " + nodeId + ", is " + setId + " but should be " + regionSetId
                );
            }
            return true;
        });
    }

    @Test
    void shouldLogProgress() {
        var graph = createTestGraph(Orientation.NATURAL);

        var testLogger = new TestProgressLogger(graph.relationshipCount(), "Wcc", 2);

        new Wcc(
            graph,
            Pools.DEFAULT,
            communitySize() / 4,
            ImmutableWccStreamConfig.builder().concurrency(2).build(),
            testLogger,
            AllocationTracker.empty()
        ).compute();

        List<AtomicLong> progresses = testLogger.getProgresses();
        assertEquals(1, progresses.size());
        assertEquals(graph.relationshipCount(), progresses.get(0).get());

        assertTrue(testLogger.containsMessage(TestLog.INFO, ":: Start"));
        assertTrue(testLogger.containsMessage(TestLog.INFO, ":: Finished"));
    }

    @Test
    void memRecParallel() {
        GraphDimensions dimensions0 = ImmutableGraphDimensions.builder().nodeCount(0).build();

        assertEquals(
            MemoryRange.of(128),
            Wcc.memoryEstimation(false).estimate(dimensions0, 1).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(168),
            Wcc.memoryEstimation(true).estimate(dimensions0, 1).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(128),
            Wcc.memoryEstimation(false).estimate(dimensions0, 8).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(168),
            Wcc.memoryEstimation(true).estimate(dimensions0, 8).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(128),
            Wcc.memoryEstimation(false).estimate(dimensions0, 64).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(168),
            Wcc.memoryEstimation(true).estimate(dimensions0, 64).memoryUsage()
        );

        GraphDimensions dimensions100 = ImmutableGraphDimensions.builder().nodeCount(100).build();
        assertEquals(
            MemoryRange.of(928),
            Wcc.memoryEstimation(false).estimate(dimensions100, 1).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(1768),
            Wcc.memoryEstimation(true).estimate(dimensions100, 1).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(928),
            Wcc.memoryEstimation(false).estimate(dimensions100, 8).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(1768),
            Wcc.memoryEstimation(true).estimate(dimensions100, 8).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(928),
            Wcc.memoryEstimation(false).estimate(dimensions100, 64).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(1768),
            Wcc.memoryEstimation(true).estimate(dimensions100, 64).memoryUsage()
        );

        GraphDimensions dimensions100B = ImmutableGraphDimensions.builder().nodeCount(100_000_000_000L).build();
        assertEquals(
            MemoryRange.of(800_122_070_456L),
            Wcc.memoryEstimation(false).estimate(dimensions100B, 1).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(1_600_244_140_824L),
            Wcc.memoryEstimation(true).estimate(dimensions100B, 1).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(800_122_070_456L),
            Wcc.memoryEstimation(false).estimate(dimensions100B, 8).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(1_600_244_140_824L),
            Wcc.memoryEstimation(true).estimate(dimensions100B, 8).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(800_122_070_456L),
            Wcc.memoryEstimation(false).estimate(dimensions100B, 64).memoryUsage()
        );
        assertEquals(
            MemoryRange.of(1_600_244_140_824L),
            Wcc.memoryEstimation(true).estimate(dimensions100B, 64).memoryUsage()
        );
    }

    private static Graph createTestGraph(Orientation orientation) {
        int[] setSizes = new int[SETS_COUNT];
        Arrays.fill(setSizes, SET_SIZE);

        StringBuilder gdl = new StringBuilder();

        for (int setSize : setSizes) {
            gdl.append(createLine(setSize));
        }

        return fromGdl(gdl.toString(), orientation);
    }

    static String createLine(int setSize) {
        return IntStream.range(0, setSize)
            .mapToObj(i -> "()")
            .collect(Collectors.joining("-[:REL]->"));
    }

    DisjointSetStruct run(Graph graph) {
        return run(graph, ImmutableWccStreamConfig.builder().build());
    }

    DisjointSetStruct run(Graph graph, WccBaseConfig config) {
        return run(graph, config, communitySize() / config.concurrency());
    }

    DisjointSetStruct run(Graph graph, WccBaseConfig config, int concurrency) {
        return new Wcc(
            graph,
            Pools.DEFAULT,
            communitySize() / concurrency,
            config,
            ProgressLogger.NULL_LOGGER,
            AllocationTracker.empty()
        ).compute();
    }
}
