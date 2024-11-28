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
import com.carrotsearch.hppc.LongHashSet;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.CommunityHelper;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.community.CommunityAlgorithms;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.WARN;

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

    static Stream<Arguments> orientationAndGraphs() {
        var monoGraph = " (a {componentId: 0})-->(b {componentId: 0})<--(c {componentId: 0})" +
                        ",(d {componentId: 1})-->(e {componentId: 1})<--(f {componentId: 1})" +
                        ",(g {componentId: 2})-->(h {componentId: 2})<--(i {componentId: 2})" +
                        ",(j {componentId: 3})-->(k {componentId: 3})<--(l {componentId: 3})" +
                        ",(m)-->(n)<--(o)";

        var unionGraph = " (a: L1 {componentId: 0})-[:A]->(b: L2 {componentId: 0})<-[:B]-(c: L1 {componentId: 0})" +
                         ",(d: L1 {componentId: 1})-[:A]->(e: L2 {componentId: 1})<-[:B]-(f: L1 {componentId: 1})" +
                         ",(g: L1 {componentId: 2})-[:A]->(h: L2 {componentId: 2})<-[:B]-(i: L1 {componentId: 2})" +
                         ",(j: L1 {componentId: 3})-[:A]->(k: L2 {componentId: 3})<-[:B]-(l: L1 {componentId: 3})" +
                         ",(m: L1)-[:A]->(n: L2)<-[:B]-(o: L1)";

        return Stream.of(
            arguments(Orientation.NATURAL, monoGraph, "natural mono"),
            arguments(Orientation.REVERSE, monoGraph, "reverse mono"),
            arguments(Orientation.UNDIRECTED, monoGraph, "undirected mono"),
            arguments(Orientation.NATURAL, unionGraph, "natural union"),
            arguments(Orientation.REVERSE, unionGraph, "reverse union"),
            arguments(Orientation.UNDIRECTED, unionGraph, "undirected union")
        );
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("orientationAndGraphs")
    void seededWccOnUnionGraphs(Orientation orientation, String gdl, @SuppressWarnings("unused") String testName) {
        var graph = fromGdl(gdl, orientation);

        var result = run(graph, new WccParameters(0D, Optional.of("componentId"), new Concurrency(4)));

        var seen = new LongHashSet();

        for (char node = 'a'; node <= 'o'; node += 3) {
            var nodeId1 = graph.toMappedNodeId(String.valueOf(node));
            var nodeId2 = graph.toMappedNodeId(String.valueOf((char) (node + 1)));
            var nodeId3 = graph.toMappedNodeId(String.valueOf((char) (node + 2)));

            var component = result.setIdOf(nodeId1);

            assertThat(component)
                .isEqualTo(result.setIdOf(nodeId2))
                .isEqualTo(result.setIdOf(nodeId3));

            // true == setId was not seen before
            assertThat(seen.add(component))
                .as("Node %s with component %d belongs to a set outside of its actual component", node, component)
                .isTrue();
        }

        assertThat(getSetCount(result)).isEqualTo(5);
    }

    @Test
    void shouldWarnAboutThresholdOnUnweightedGraphs() {
        var log = new GdsTestLog();
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .with(EmptyTaskRegistryFactory.INSTANCE)
            .with(TerminationFlag.RUNNING_TRUE)
            .with(EmptyUserLogRegistryFactory.INSTANCE)
            .build();
        var progressTrackerCreator = new ProgressTrackerCreator(log, requestScopedDependencies);
        var communityAlgorithms = new CommunityAlgorithms(progressTrackerCreator, requestScopedDependencies.getTerminationFlag());

        var graph = createTestGraph(Orientation.NATURAL);
        var configuration = WccStreamConfigImpl.builder().relationshipWeightProperty("weights").build();
        communityAlgorithms.wcc(graph, configuration);

        Assertions.assertThat(log.getMessages(WARN))
            .extracting(removingThreadId())
            .containsExactly("WCC :: Specifying a `relationshipWeightProperty` has no effect unless `threshold` is also set.");
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
        return run(graph, new WccParameters(0D, new Concurrency(4)));
    }

    DisjointSetStruct run(Graph graph, WccParameters parameters) {
        return run(graph, parameters, communitySize() / parameters.concurrency().value());
    }

    DisjointSetStruct run(Graph graph, WccParameters parameters, int concurrency) {
        return new Wcc(
            graph,
            DefaultPool.INSTANCE,
            communitySize() / concurrency,
            parameters,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
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

        @GdlGraph(orientation = Orientation.NATURAL, graphNamePrefix = "indexed")
        private static final String INDEXED = TEST_GRAPH;

        @Inject
        private TestGraph naturalGraph;

        @Inject
        private TestGraph reverseGraph;

        @Inject
        private TestGraph undirectedGraph;

        @Inject
        private TestGraph indexedGraph;

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

        @Test
        void computeIndexed() {
            assertResults(indexedGraph);
        }

        private void assertResults(TestGraph graph) {
            var wccStub = new WccStub(TerminationFlag.RUNNING_TRUE, new AlgorithmMachinery());

            var parameters = new WccParameters(0D, new Concurrency(4));
            var dss = wccStub.wcc(graph, parameters, ProgressTracker.NULL_TRACKER, true);

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
            return Arrays.stream(nodes).map(graph::toMappedNodeId).collect(Collectors.toList());
        }
    }
}
