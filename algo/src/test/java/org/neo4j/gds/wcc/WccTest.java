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
package org.neo4j.gds.wcc;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.CommunityHelper;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProgressLogger;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistry;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.logging.NullLog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.TestLog.INFO;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

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

    @ParameterizedTest
    @EnumSource(Orientation.class)
    void shouldLogProgress(Orientation orientation) {
        var graph = createTestGraph(orientation);

        var wcc = new WccAlgorithmFactory<>(TestProgressLogger.FACTORY).build(
            graph,
            ImmutableWccStreamConfig.builder().concurrency(2).build(),
            AllocationTracker.empty(),
            NullLog.getInstance(),
            EmptyTaskRegistry.INSTANCE
        );
        wcc.compute();

        var messagesInOrder = ((TestProgressLogger) wcc.getProgressTracker().progressLogger()).getMessages(INFO);

        assertThat(messagesInOrder)
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            .hasSize(103)
            .containsSequence(
                "WCC :: Start",
                "WCC 0%",
                "WCC 1%",
                "WCC 2%"
            )
            .containsSequence(
                "WCC 98%",
                "WCC 99%",
                "WCC 100%",
                "WCC :: Finished"
            );
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
            ProgressTracker.NULL_TRACKER,
            AllocationTracker.empty()
        ).compute();
    }

    @Nested
    @GdlExtension
    @TestInstance(value = TestInstance.Lifecycle.PER_CLASS)
    class Gdl {

        @GdlGraph(orientation = Orientation.NATURAL, graphNamePrefix = "natural")
        private static final String TEST_GRAPH =
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node)" +
            ", (e:Node)" +
            ", (f:Node)" +
            ", (g:Node)" +
            ", (h:Node)" +
            ", (i:Node)" +
            // {J}
            ", (j:Node)" +
            // {A, B, C, D}
            ", (a)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(d)" +
            ", (d)-[:TYPE]->(a)" +
            // {E, F, G}
            ", (e)-[:TYPE]->(f)" +
            ", (f)-[:TYPE]->(g)" +
            ", (g)-[:TYPE]->(e)" +
            // {H, I}
            ", (i)-[:TYPE]->(h)" +
            ", (h)-[:TYPE]->(i)";


        @GdlGraph(orientation = Orientation.REVERSE, graphNamePrefix = "reverse")
        private static final String REVERSE = TEST_GRAPH;

        @GdlGraph(orientation = Orientation.UNDIRECTED, graphNamePrefix = "undirected")
        private static final String UNDIRECTED = TEST_GRAPH;

        @Inject
        private TestGraph naturalGraph;

        @Inject
        private TestGraph reverseGraph;

        @Inject
        private TestGraph undirectedGraph;

        @Test
        void computeNatural() {
            assertResults(naturalGraph);
        }

        @Test
        void computeReverse() {
            assertResults(reverseGraph);
        }

        @Test
        void computeUndirected() {
            assertResults(undirectedGraph);
        }

        private void assertResults(TestGraph graph) {
            var config = ImmutableWccStreamConfig.builder().build();

            var dss = new WccAlgorithmFactory<>()
                .build(
                    graph,
                    config,
                    AllocationTracker.empty(),
                    ProgressTracker.NULL_TRACKER
                ).compute();


            var actualCommunities = new ArrayList<Long>();
            graph.forEachNode(node -> actualCommunities.add(dss.setIdOf(node)));
            CommunityHelper.assertCommunities(
                actualCommunities,
                List.of(
                    ids( graph, "a", "b", "c", "d"),
                    ids( graph, "e", "f", "g"),
                    ids( graph, "h", "i"),
                    ids( graph, "j")
                )
            );
        }

        private List<Long> ids(TestGraph graph, String... nodes) {
            return Arrays.stream(nodes).map(graph::toOriginalNodeId).collect(Collectors.toList());
        }
    }
}
