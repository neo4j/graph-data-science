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
package org.neo4j.gds.similarity.nodesim;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.REVERSE;
import static org.neo4j.gds.Orientation.UNDIRECTED;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.crossArguments;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.TestSupport.toArguments;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;
import static org.neo4j.gds.similarity.nodesim.NodeSimilarityBaseConfig.TOP_K_DEFAULT;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
final class NodeSimilarityTest {

    @GdlGraph(graphNamePrefix = "natural", orientation = NATURAL)
    @GdlGraph(graphNamePrefix = "reverse", orientation = REVERSE)
    @GdlGraph(graphNamePrefix = "undirected", orientation = UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Person)" +
        ", (b:Person)" +
        ", (c:Person)" +
        ", (d:Person)" +
        ", (i1:Item)" +
        ", (i2:Item)" +
        ", (i3:Item)" +
        ", (i4:Item)" +
        ", (a)-[:LIKES {prop: 1.0}]->(i1)" +
        ", (a)-[:LIKES {prop: 1.0}]->(i2)" +
        ", (a)-[:LIKES {prop: 2.0}]->(i3)" +
        ", (b)-[:LIKES {prop: 1.0}]->(i1)" +
        ", (b)-[:LIKES {prop: 1.0}]->(i2)" +
        ", (c)-[:LIKES {prop: 1.0}]->(i3)" +
        ", (d)-[:LIKES {prop: 0.5}]->(i1)" +
        ", (d)-[:LIKES {prop: 1.0}]->(i2)" +
        ", (d)-[:LIKES {prop: 1.0}]->(i3)";

    @GdlGraph(graphNamePrefix = "naturalUnion", orientation = NATURAL)
    private static final String DB_CYPHER_UNION =
        "CREATE" +
        "  (a:Person)" +
        ", (b:Person)" +
        ", (c:Person)" +
        ", (d:Person)" +
        ", (i1:Item)" +
        ", (i2:Item)" +
        ", (i3:Item)" +
        ", (i4:Item)" +
        ", (a)-[:LIKES3 {prop: 1.0}]->(i1)" +
        ", (a)-[:LIKES2 {prop: 1.0}]->(i2)" +
        ", (a)-[:LIKES1 {prop: 2.0}]->(i3)" +
        ", (b)-[:LIKES2 {prop: 1.0}]->(i1)" +
        ", (b)-[:LIKES1 {prop: 1.0}]->(i2)" +
        ", (c)-[:LIKES3 {prop: 1.0}]->(i3)" +
        ", (d)-[:LIKES2 {prop: 0.5}]->(i1)" +
        ", (d)-[:LIKES3 {prop: 1.0}]->(i2)" +
        ", (d)-[:LIKES1 {prop: 1.0}]->(i3)";

    @Inject
    private TestGraph naturalGraph;

    @Inject
    private TestGraph reverseGraph;

    @Inject
    private TestGraph undirectedGraph;

    @Inject
    private TestGraph naturalUnionGraph;

    private static final Collection<String> EXPECTED_OUTGOING = new HashSet<>();
    private static final Collection<String> EXPECTED_INCOMING = new HashSet<>();

    private static final Collection<String> EXPECTED_WEIGHTED_OUTGOING = new HashSet<>();
    private static final Collection<String> EXPECTED_WEIGHTED_INCOMING = new HashSet<>();

    private static final Collection<String> EXPECTED_OUTGOING_TOP_N_1 = new HashSet<>();
    private static final Collection<String> EXPECTED_INCOMING_TOP_N_1 = new HashSet<>();

    private static final Collection<String> EXPECTED_OUTGOING_TOP_K_1 = new HashSet<>();
    private static final Collection<String> EXPECTED_INCOMING_TOP_K_1 = new HashSet<>();

    private static final Collection<String> EXPECTED_OUTGOING_SIMILARITY_CUTOFF = new HashSet<>();
    private static final Collection<String> EXPECTED_INCOMING_SIMILARITY_CUTOFF = new HashSet<>();

    private static final Collection<String> EXPECTED_OUTGOING_DEGREE_CUTOFF = new HashSet<>();
    private static final Collection<String> EXPECTED_INCOMING_DEGREE_CUTOFF = new HashSet<>();

    private static final int COMPARED_ITEMS = 3;
    private static final int COMPARED_PERSONS = 4;

    static ImmutableNodeSimilarityWriteConfig.Builder configBuilder() {
        return ImmutableNodeSimilarityWriteConfig
            .builder()
            .writeProperty("writeProperty")
            .writeRelationshipType("writeRelationshipType")
            .similarityCutoff(0.0);
    }

    private static String resultString(long node1, long node2, double similarity) {
        return formatWithLocale("%d,%d %f", node1, node2, similarity);
    }

    private static String resultString(SimilarityResult result) {
        return resultString(result.node1, result.node2, result.similarity);
    }

    private static Stream<Integer> concurrencies() {
        return Stream.of(1, 4);
    }

    static {
        EXPECTED_OUTGOING.add(resultString(0, 1, 2 / 3.0));
        EXPECTED_OUTGOING.add(resultString(0, 2, 1 / 3.0));
        EXPECTED_OUTGOING.add(resultString(0, 3, 1.0));
        EXPECTED_OUTGOING.add(resultString(1, 2, 0.0));
        EXPECTED_OUTGOING.add(resultString(1, 3, 2 / 3.0));
        EXPECTED_OUTGOING.add(resultString(2, 3, 1 / 3.0));
        // Add results in reverse direction because topK
        EXPECTED_OUTGOING.add(resultString(1, 0, 2 / 3.0));
        EXPECTED_OUTGOING.add(resultString(2, 0, 1 / 3.0));
        EXPECTED_OUTGOING.add(resultString(3, 0, 1.0));
        EXPECTED_OUTGOING.add(resultString(2, 1, 0.0));
        EXPECTED_OUTGOING.add(resultString(3, 1, 2 / 3.0));
        EXPECTED_OUTGOING.add(resultString(3, 2, 1 / 3.0));

        EXPECTED_WEIGHTED_OUTGOING.add(resultString(0, 1, 2 / 4.0));
        EXPECTED_WEIGHTED_OUTGOING.add(resultString(0, 2, 1 / 4.0));
        EXPECTED_WEIGHTED_OUTGOING.add(resultString(0, 3, 2.5 / 4.0));
        EXPECTED_WEIGHTED_OUTGOING.add(resultString(1, 2, 0.0));
        EXPECTED_WEIGHTED_OUTGOING.add(resultString(1, 3, 2 / 4.0));
        EXPECTED_WEIGHTED_OUTGOING.add(resultString(2, 3, 1 / 2.5));
        // Add results in reverse direction because topK
        EXPECTED_WEIGHTED_OUTGOING.add(resultString(1, 0, 2 / 4.0));
        EXPECTED_WEIGHTED_OUTGOING.add(resultString(2, 0, 1 / 4.0));
        EXPECTED_WEIGHTED_OUTGOING.add(resultString(3, 0, 2.5 / 4.0));
        EXPECTED_WEIGHTED_OUTGOING.add(resultString(2, 1, 0.0));
        EXPECTED_WEIGHTED_OUTGOING.add(resultString(3, 1, 2 / 4.0));
        EXPECTED_WEIGHTED_OUTGOING.add(resultString(3, 2, 1 / 2.5));

        EXPECTED_OUTGOING_TOP_N_1.add(resultString(0, 3, 1.0));

        EXPECTED_OUTGOING_TOP_K_1.add(resultString(0, 3, 1.0));
        EXPECTED_OUTGOING_TOP_K_1.add(resultString(1, 0, 2 / 3.0));
        EXPECTED_OUTGOING_TOP_K_1.add(resultString(2, 0, 1 / 3.0));
        EXPECTED_OUTGOING_TOP_K_1.add(resultString(3, 0, 1.0));

        EXPECTED_OUTGOING_SIMILARITY_CUTOFF.add(resultString(0, 1, 2 / 3.0));
        EXPECTED_OUTGOING_SIMILARITY_CUTOFF.add(resultString(0, 2, 1 / 3.0));
        EXPECTED_OUTGOING_SIMILARITY_CUTOFF.add(resultString(0, 3, 1.0));
        EXPECTED_OUTGOING_SIMILARITY_CUTOFF.add(resultString(1, 3, 2 / 3.0));
        EXPECTED_OUTGOING_SIMILARITY_CUTOFF.add(resultString(2, 3, 1 / 3.0));
        // Add results in reverse direction because topK
        EXPECTED_OUTGOING_SIMILARITY_CUTOFF.add(resultString(1, 0, 2 / 3.0));
        EXPECTED_OUTGOING_SIMILARITY_CUTOFF.add(resultString(2, 0, 1 / 3.0));
        EXPECTED_OUTGOING_SIMILARITY_CUTOFF.add(resultString(3, 0, 1.0));
        EXPECTED_OUTGOING_SIMILARITY_CUTOFF.add(resultString(3, 1, 2 / 3.0));
        EXPECTED_OUTGOING_SIMILARITY_CUTOFF.add(resultString(3, 2, 1 / 3.0));

        EXPECTED_OUTGOING_DEGREE_CUTOFF.add(resultString(0, 1, 2 / 3.0));
        EXPECTED_OUTGOING_DEGREE_CUTOFF.add(resultString(0, 3, 1.0));
        EXPECTED_OUTGOING_DEGREE_CUTOFF.add(resultString(1, 3, 2 / 3.0));
        // Add results in reverse direction because topK
        EXPECTED_OUTGOING_DEGREE_CUTOFF.add(resultString(1, 0, 2 / 3.0));
        EXPECTED_OUTGOING_DEGREE_CUTOFF.add(resultString(3, 0, 1.0));
        EXPECTED_OUTGOING_DEGREE_CUTOFF.add(resultString(3, 1, 2 / 3.0));

        EXPECTED_INCOMING.add(resultString(4, 5, 1.0));
        EXPECTED_INCOMING.add(resultString(4, 6, 1 / 2.0));
        EXPECTED_INCOMING.add(resultString(5, 6, 1 / 2.0));
        // Add results in reverse direction because topK
        EXPECTED_INCOMING.add(resultString(5, 4, 1.0));
        EXPECTED_INCOMING.add(resultString(6, 4, 1 / 2.0));
        EXPECTED_INCOMING.add(resultString(6, 5, 1 / 2.0));

        EXPECTED_WEIGHTED_INCOMING.add(resultString(4, 5, 2.5 / 3.0));
        EXPECTED_WEIGHTED_INCOMING.add(resultString(4, 6, 1.5 / 5.0));
        EXPECTED_WEIGHTED_INCOMING.add(resultString(5, 6, 2.0 / 5.0));
        // Add results in reverse direction because topK
        EXPECTED_WEIGHTED_INCOMING.add(resultString(5, 4, 2.5 / 3.0));
        EXPECTED_WEIGHTED_INCOMING.add(resultString(6, 4, 1.5 / 5.0));
        EXPECTED_WEIGHTED_INCOMING.add(resultString(6, 5, 2.0 / 5.0));

        EXPECTED_INCOMING_TOP_N_1.add(resultString(4, 5, 3.0 / 3.0));

        EXPECTED_INCOMING_TOP_K_1.add(resultString(4, 5, 1.0));
        EXPECTED_INCOMING_TOP_K_1.add(resultString(5, 4, 1.0));
        EXPECTED_INCOMING_TOP_K_1.add(resultString(6, 4, 1 / 2.0));

        EXPECTED_INCOMING_SIMILARITY_CUTOFF.add(resultString(4, 5, 1.0));
        EXPECTED_INCOMING_SIMILARITY_CUTOFF.add(resultString(4, 6, 1 / 2.0));
        EXPECTED_INCOMING_SIMILARITY_CUTOFF.add(resultString(5, 6, 1 / 2.0));
        // Add results in reverse direction because topK
        EXPECTED_INCOMING_SIMILARITY_CUTOFF.add(resultString(5, 4, 1.0));
        EXPECTED_INCOMING_SIMILARITY_CUTOFF.add(resultString(6, 4, 1 / 2.0));
        EXPECTED_INCOMING_SIMILARITY_CUTOFF.add(resultString(6, 5, 1 / 2.0));

        EXPECTED_INCOMING_DEGREE_CUTOFF.add(resultString(4, 5, 3.0 / 3.0));
        EXPECTED_INCOMING_DEGREE_CUTOFF.add(resultString(4, 6, 1 / 2.0));
        EXPECTED_INCOMING_DEGREE_CUTOFF.add(resultString(5, 6, 1 / 2.0));
        // Add results in reverse direction because topK
        EXPECTED_INCOMING_DEGREE_CUTOFF.add(resultString(5, 4, 3.0 / 3.0));
        EXPECTED_INCOMING_DEGREE_CUTOFF.add(resultString(6, 4, 1 / 2.0));
        EXPECTED_INCOMING_DEGREE_CUTOFF.add(resultString(6, 5, 1 / 2.0));
    }

    static Stream<Arguments> supportedLoadAndComputeDirections() {
        Stream<Arguments> directions = Stream.of(
            arguments(NATURAL),
            arguments(REVERSE)
        );
        return crossArguments(() -> directions, toArguments(NodeSimilarityTest::concurrencies));
    }

    static Stream<Arguments> topKAndConcurrencies() {
        Stream<Integer> topKStream = Stream.of(TOP_K_DEFAULT, 100);
        return TestSupport.crossArguments(
            toArguments(() -> topKStream),
            toArguments(NodeSimilarityTest::concurrencies)
        );
    }

    @ParameterizedTest(name = "orientation: {0}, concurrency: {1}")
    @MethodSource("supportedLoadAndComputeDirections")
    void shouldComputeWeightedForSupportedDirections(Orientation orientation, int concurrency) {
        Graph graph = orientation == NATURAL ? naturalGraph : reverseGraph;

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder().relationshipWeightProperty("prop").concurrency(concurrency).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        Set<String> result = nodeSimilarity
            .computeToStream()
            .map(NodeSimilarityTest::resultString)
            .collect(Collectors.toSet());

        assertEquals(orientation == REVERSE ? EXPECTED_WEIGHTED_INCOMING : EXPECTED_WEIGHTED_OUTGOING, result);
    }

    @ParameterizedTest(name = "orientation: {0}, concurrency: {1}")
    @MethodSource("supportedLoadAndComputeDirections")
    void shouldComputeForSupportedDirections(Orientation orientation, int concurrency) {
        Graph graph = orientation == NATURAL ? naturalGraph : reverseGraph;

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder().concurrency(concurrency).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        Set<String> result = nodeSimilarity
            .computeToStream()
            .map(NodeSimilarityTest::resultString)
            .collect(Collectors.toSet());

        assertEquals(orientation == REVERSE ? EXPECTED_INCOMING : EXPECTED_OUTGOING, result);
    }

    @ParameterizedTest(name = "orientation: {0}, concurrency: {1}")
    @MethodSource("supportedLoadAndComputeDirections")
    void shouldComputeTopNForSupportedDirections(Orientation orientation, int concurrency) {
        Graph graph = orientation == NATURAL ? naturalGraph : reverseGraph;

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder().concurrency(concurrency).topN(1).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        Set<String> result = nodeSimilarity
            .computeToStream()
            .map(NodeSimilarityTest::resultString)
            .collect(Collectors.toSet());

        assertEquals(orientation == REVERSE ? EXPECTED_INCOMING_TOP_N_1 : EXPECTED_OUTGOING_TOP_N_1, result);
    }

    @ParameterizedTest(name = "orientation: {0}, concurrency: {1}")
    @MethodSource("supportedLoadAndComputeDirections")
    void shouldComputeNegativeTopNForSupportedDirections(Orientation orientation, int concurrency) {
        Graph graph = orientation == NATURAL ? naturalGraph : reverseGraph;

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder().concurrency(concurrency).bottomN(1).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        Graph similarityGraph = nodeSimilarity.computeToGraph().similarityGraph();

        assertGraphEquals(
            orientation == REVERSE ? fromGdl(
                "(i1:Item)-[:REL {w: 0.50000D}]->(i3:Item), (i2:Item), (i4:Item), (a:Person), (b:Person), (c:Person), (d:Person)") : fromGdl(
                "(a:Person), (b:Person)-[:REL {w: 0.00000D}]->(c:Person), (d:Person), (i1:Item), (i2:Item), (i3:Item), (i4:Item)"),
            similarityGraph
        );
    }

    @ParameterizedTest(name = "orientation: {0}, concurrency: {1}")
    @MethodSource("supportedLoadAndComputeDirections")
    void shouldComputeTopKForSupportedDirections(Orientation orientation, int concurrency) {
        Graph graph = orientation == NATURAL ? naturalGraph : reverseGraph;

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder().topK(1).concurrency(concurrency).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        Set<String> result = nodeSimilarity
            .computeToStream()
            .map(NodeSimilarityTest::resultString)
            .collect(Collectors.toSet());

        assertEquals(orientation == REVERSE ? EXPECTED_INCOMING_TOP_K_1 : EXPECTED_OUTGOING_TOP_K_1, result);
    }

    @ParameterizedTest(name = "orientation: {0}, concurrency: {1}")
    @MethodSource("supportedLoadAndComputeDirections")
    void shouldComputeNegativeTopKForSupportedDirections(Orientation orientation, int concurrency) {
        Graph graph = orientation == NATURAL ? naturalGraph : reverseGraph;

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder()
                .concurrency(concurrency)
                .topK(10)
                .bottomK(1)
                .build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        Graph similarityGraph = nodeSimilarity.computeToGraph().similarityGraph();

        assertGraphEquals(
            orientation == REVERSE
                ? fromGdl("  (i1:Item)-[:LIKES {w: 0.50000D}]->(i3:Item)" +
                          ", (i2:Item)-[:LIKES {w: 0.50000D}]->(i3)" +
                          ", (i3)-[:LIKES {w: 0.500000D}]->(i1)" +
                          ", (i4:Item)" +
                          ", (:Person), (:Person), (:Person), (:Person)")
                : fromGdl("  (a:Person)-[:LIKES {w: 0.333333D}]->(c:Person)" +
                          ", (b:Person)-[:LIKES {w: 0.00000D}]->(c)" +
                          ", (c)-[:LIKES {w: 0.000000D}]->(b)" +
                          ", (d:Person)-[:LIKES {w: 0.333333D}]->(c)" +
                          ", (:Item), (:Item), (:Item), (:Item)"),
            similarityGraph
        );
    }

    @ParameterizedTest(name = "orientation: {0}, concurrency: {1}")
    @MethodSource("supportedLoadAndComputeDirections")
    void shouldComputeWithSimilarityCutoffForSupportedDirections(Orientation orientation, int concurrency) {
        Graph graph = orientation == NATURAL ? naturalGraph : reverseGraph;

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder().concurrency(concurrency).similarityCutoff(0.1).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        Set<String> result = nodeSimilarity
            .computeToStream()
            .map(NodeSimilarityTest::resultString)
            .collect(Collectors.toSet());

        assertEquals(
            orientation == REVERSE ? EXPECTED_INCOMING_SIMILARITY_CUTOFF : EXPECTED_OUTGOING_SIMILARITY_CUTOFF,
            result
        );
    }

    @ParameterizedTest(name = "orientation: {0}, concurrency: {1}")
    @MethodSource("supportedLoadAndComputeDirections")
    void shouldComputeWithDegreeCutoffForSupportedDirections(Orientation orientation, int concurrency) {
        Graph graph = orientation == NATURAL ? naturalGraph : reverseGraph;

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder().degreeCutoff(2).concurrency(concurrency).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        Set<String> result = nodeSimilarity
            .computeToStream()
            .map(NodeSimilarityTest::resultString)
            .collect(Collectors.toSet());

        assertEquals(
            orientation == REVERSE ? EXPECTED_INCOMING_DEGREE_CUTOFF : EXPECTED_OUTGOING_DEGREE_CUTOFF,
            result
        );
    }

    @ParameterizedTest(name = "concurrency = {0}")
    @MethodSource("concurrencies")
    void shouldComputeForUndirectedGraphs(int concurrency) {
        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            undirectedGraph,
            configBuilder().concurrency(concurrency).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );
        Set<SimilarityResult> result = nodeSimilarity.computeToStream().collect(Collectors.toSet());
        assertNotEquals(Collections.emptySet(), result);
    }

    @Test
    void shouldComputeForUnionGraphs() {
        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            naturalGraph,
            configBuilder().concurrency(1).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );
        var result1 = nodeSimilarity.computeToStream().collect(Collectors.toSet());

        nodeSimilarity = NodeSimilarity.create(
            naturalUnionGraph,
            configBuilder().concurrency(1).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );
        var result2 = nodeSimilarity.computeToStream().collect(Collectors.toSet());

        assertEquals(result1, result2);
    }

    @ParameterizedTest(name = "orientation: {0}, concurrency: {1}")
    @MethodSource("supportedLoadAndComputeDirections")
    void shouldComputeSimilarityGraphInAllSupportedDirections(Orientation orientation, int concurrency) {
        Graph graph = orientation == NATURAL ? naturalGraph : reverseGraph;

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder().concurrency(concurrency).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        SimilarityGraphResult similarityGraphResult = nodeSimilarity.computeToGraph();
        assertEquals(
            orientation == REVERSE ? COMPARED_ITEMS : COMPARED_PERSONS,
            similarityGraphResult.comparedNodes()
        );
        Graph resultGraph = similarityGraphResult.similarityGraph();
        assertGraphEquals(
            orientation == REVERSE
                ? fromGdl("  (:Person), (:Person), (:Person), (:Person)" +
                          ", (:Item)" +
                          ", (i1:Item)-[:LIKES {property: 1.000000D}]->(i2:Item)" +
                          ", (i1)-[:LIKES {property: 0.500000D}]->(i3:Item)" +
                          ", (i2)-[:LIKES {property: 0.500000D}]->(i3)" +
                          // Add results in reverse direction because topK
                          ", (i2)-[:LIKES {property: 1.000000D}]->(i1)" +
                          ", (i3)-[:LIKES {property: 0.500000D}]->(i1)" +
                          ", (i3)-[:LIKES {property: 0.500000D}]->(i2)")
                : fromGdl("  (p1:Person)-[:LIKES {property: 0.666667D}]->(p2:Person)" +
                          ", (p1)-[:LIKES {property: 0.333333D}]->(p3:Person)" +
                          ", (p1)-[:LIKES {property: 1.000000D}]->(p4:Person)" +
                          ", (p2)-[:LIKES {property: 0.000000D}]->(p3)" +
                          ", (p2)-[:LIKES {property: 0.666667D}]->(p4)" +
                          ", (p3)-[:LIKES {property: 0.333333D}]->(p4)" +
                          // Add results in reverse direction because topK
                          "  (p2)-[:LIKES {property: 0.666667D}]->(p1)" +
                          ", (p3)-[:LIKES {property: 0.333333D}]->(p1)" +
                          ", (p4)-[:LIKES {property: 1.000000D}]->(p1)" +
                          ", (p3)-[:LIKES {property: 0.000000D}]->(p2)" +
                          ", (p4)-[:LIKES {property: 0.666667D}]->(p2)" +
                          ", (p4)-[:LIKES {property: 0.333333D}]->(p3)" +
                          ", (:Item), (:Item), (:Item), (:Item)"),
            resultGraph
        );
    }

    @ParameterizedTest(name = "orientation: {0}, concurrency: {1}")
    @MethodSource("supportedLoadAndComputeDirections")
    void shouldComputeToGraphWithUnusedNodesInInputGraph(Orientation orientation, int concurrency) {
        Graph graph = fromGdl(DB_CYPHER + ", (:Unused)".repeat(1024), orientation);

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder()
                .concurrency(concurrency)
                .topK(100)
                .topN(1)
                .build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        SimilarityGraphResult similarityGraphResult = nodeSimilarity.computeToGraph();
        assertEquals(
            orientation == REVERSE ? COMPARED_ITEMS : COMPARED_PERSONS,
            similarityGraphResult.comparedNodes()
        );

        Graph resultGraph = similarityGraphResult.similarityGraph();
        String expected = orientation == REVERSE ? resultString(4, 5, 1.00000) : resultString(
            0,
            3,
            1.00000
        );

        resultGraph.forEachNode(n -> {
            resultGraph.forEachRelationship(n, -1.0, (s, t, w) -> {
                assertEquals(expected, resultString(s, t, w));
                return true;
            });
            return true;
        });

    }

    @ParameterizedTest(name = "orientation: {0}, concurrency: {1}")
    @MethodSource("supportedLoadAndComputeDirections")
    void shouldIgnoreLoops(Orientation orientation, int concurrency) {
        // Add loops
        var gdl = DB_CYPHER + ", (a)-[:LIKES {prop: 1.0}]->(a), (i1)-[:LIKES {prop: 1.0}]->(i1)";

        Graph graph = fromGdl(gdl, orientation);

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder().concurrency(concurrency).topN(1).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        Set<String> result = nodeSimilarity
            .computeToStream()
            .map(NodeSimilarityTest::resultString)
            .collect(Collectors.toSet());

        assertEquals(orientation == REVERSE ? EXPECTED_INCOMING_TOP_N_1 : EXPECTED_OUTGOING_TOP_N_1, result);
    }

    @ParameterizedTest(name = "orientation: {0}, concurrency: {1}")
    @MethodSource("supportedLoadAndComputeDirections")
    void shouldIgnoreParallelEdges(Orientation orientation, int concurrency) {
        // Add parallel edges
        var gdl = DB_CYPHER +
                  ", (a)-[:LIKES {prop: 1.0}]->(i1)" +
                  ", (c)-[:LIKES {prop: 1.0}]->(i3)" +
                  ", (c)-[:LIKES {prop: 1.0}]->(i3)" +
                  ", (c)-[:LIKES {prop: 1.0}]->(i3)";

        Graph graph = fromGdl(gdl, orientation);

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder().concurrency(concurrency).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        Set<String> result = nodeSimilarity
            .computeToStream()
            .map(NodeSimilarityTest::resultString)
            .collect(Collectors.toSet());

        assertEquals(orientation == REVERSE ? EXPECTED_INCOMING : EXPECTED_OUTGOING, result);
    }

    @ParameterizedTest(name = "topK = {0}")
    @ValueSource(ints = {TOP_K_DEFAULT, 100})
    void shouldComputeMemrec(int topK) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(1_000_000)
            .relCountUpperBound(5_000_000)
            .build();

        NodeSimilarityWriteConfig config = ImmutableNodeSimilarityWriteConfig
            .builder()
            .similarityCutoff(0.0)
            .topK(topK)
            .writeProperty("writeProperty")
            .writeRelationshipType("writeRelationshipType")
            .build();

        MemoryTree actual = new NodeSimilarityFactory<>().memoryEstimation(config).estimate(dimensions, 1);

        long nodeFilterRangeMin = 125_016L;
        long nodeFilterRangeMax = 125_016L;
        MemoryRange nodeFilterRange = MemoryRange.of(nodeFilterRangeMin, nodeFilterRangeMax);

        long vectorsRangeMin = 56_000_016L;
        long vectorsRangeMax = 56_000_016L;
        MemoryRange vectorsRange = MemoryRange.of(vectorsRangeMin, vectorsRangeMax);

        long weightsRangeMin = 16L;
        long weightsRangeMax = 56_000_016L;
        MemoryRange weightsRange = MemoryRange.of(weightsRangeMin, weightsRangeMax);

        MemoryEstimations.Builder builder = MemoryEstimations.builder()
            .fixed("node filter", nodeFilterRange)
            .fixed("vectors", vectorsRange)
            .fixed("weights", weightsRange)
            .fixed("similarityComputer", 8);

        long topKMapRangeMin;
        long topKMapRangeMax;
        if (topK == TOP_K_DEFAULT) {
            topKMapRangeMin = 248_000_016L;
            topKMapRangeMax = 248_000_016L;
        } else {
            topKMapRangeMin = 1_688_000_016L;
            topKMapRangeMax = 1_688_000_016L;
        }
        builder.fixed("topK map", MemoryRange.of(topKMapRangeMin, topKMapRangeMax));

        MemoryTree expected = builder.build().estimate(dimensions, 1);

        assertEquals(expected.memoryUsage(), actual.memoryUsage());
    }

    @ParameterizedTest(name = "topK = {0}")
    @ValueSource(ints = {TOP_K_DEFAULT, 100})
    void shouldComputeMemrecWithTop(int topK) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(1_000_000)
            .relCountUpperBound(5_000_000)
            .build();

        NodeSimilarityWriteConfig config = ImmutableNodeSimilarityWriteConfig
            .builder()
            .similarityCutoff(0.0)
            .topN(100)
            .topK(topK)
            .writeProperty("writeProperty")
            .writeRelationshipType("writeRelationshipType")
            .build();

        MemoryTree actual = new NodeSimilarityFactory<>().memoryEstimation(config).estimate(dimensions, 1);

        long nodeFilterRangeMin = 125_016L;
        long nodeFilterRangeMax = 125_016L;
        MemoryRange nodeFilterRange = MemoryRange.of(nodeFilterRangeMin, nodeFilterRangeMax);

        long vectorsRangeMin = 56_000_016L;
        long vectorsRangeMax = 56_000_016L;
        MemoryRange vectorsRange = MemoryRange.of(vectorsRangeMin, vectorsRangeMax);

        long weightsRangeMin = 16L;
        long weightsRangeMax = 56_000_016L;
        MemoryRange weightsRange = MemoryRange.of(weightsRangeMin, weightsRangeMax);

        long topNListMin = 2_504L;
        long topNListMax = 2_504L;
        MemoryRange topNListRange = MemoryRange.of(topNListMin, topNListMax);

        MemoryEstimations.Builder builder = MemoryEstimations.builder()
            .fixed("node filter", nodeFilterRange)
            .fixed("vectors", vectorsRange)
            .fixed("weights", weightsRange)
            .fixed("topNList", topNListRange)
            .fixed("similarityComputer", 8);

        long topKMapRangeMin;
        long topKMapRangeMax;
        if (topK == TOP_K_DEFAULT) {
            topKMapRangeMin = 248_000_016L;
            topKMapRangeMax = 248_000_016L;
        } else {
            topKMapRangeMin = 1_688_000_016L;
            topKMapRangeMax = 1_688_000_016L;
        }
        builder.fixed("topK map", MemoryRange.of(topKMapRangeMin, topKMapRangeMax));

        MemoryTree expected = builder.build().estimate(dimensions, 1);

        assertEquals(expected.memoryUsage(), actual.memoryUsage());
    }

    @Test
    void shouldComputeMemrecWithTopKAndTopNGreaterThanNodeCount() {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100)
            .relCountUpperBound(20_000)
            .build();

        NodeSimilarityWriteConfig config = ImmutableNodeSimilarityWriteConfig
            .builder()
            .similarityCutoff(0.0)
            .topK(Integer.MAX_VALUE)
            .topN(Integer.MAX_VALUE)
            .writeProperty("writeProperty")
            .writeRelationshipType("writeRelationshipType")
            .build();

        MemoryTree actual = new NodeSimilarityFactory<>().memoryEstimation(config).estimate(dimensions, 1);

        assertEquals(570592, actual.memoryUsage().min);
        assertEquals(732192, actual.memoryUsage().max);

    }

    @ParameterizedTest(name = "topK = {0}, concurrency = {1}")
    @MethodSource("topKAndConcurrencies")
    void shouldLogMessages(int topK, int concurrency) {
        var graph = naturalGraph;
        var config = configBuilder().topN(100).topK(topK).concurrency(concurrency).build();

        var progressLog = Neo4jProxy.testLog();
        var nodeSimilarity = new NodeSimilarityFactory<>().build(
            graph,
            config,
            progressLog,
            EmptyTaskRegistryFactory.INSTANCE
        );

        nodeSimilarity.compute();

        assertThat(progressLog.getMessages(INFO))
            .extracting(removingThreadId())
            .contains(
                "NodeSimilarity :: prepare :: Start",
                "NodeSimilarity :: prepare :: Finished",
                "NodeSimilarity :: compare node pairs :: Start",
                "NodeSimilarity :: compare node pairs :: Finished"
            );
    }

    @ParameterizedTest(name = "topK = {0}, concurrency = {1}")
    @MethodSource("topKAndConcurrencies")
    void shouldNotLogMessagesWhenLoggingIsDisabled(int topK, int concurrency) {
        var graph = naturalGraph;
        var config = configBuilder().topN(100).topK(topK).concurrency(concurrency).logProgress(false).build();

        var progressLog = Neo4jProxy.testLog();
        var nodeSimilarity = new NodeSimilarityFactory<>().build(
            graph,
            config,
            progressLog,
            EmptyTaskRegistryFactory.INSTANCE
        );

        nodeSimilarity.compute();

        assertThat(progressLog.getMessages(INFO))
            .as("When progress logging is disabled we only log `start` and `finished`.")
            .extracting(removingThreadId())
            .containsExactly(
                "NodeSimilarity :: Start",
                "NodeSimilarity :: prepare :: Start",
                "NodeSimilarity :: prepare :: Finished",
                "NodeSimilarity :: compare node pairs :: Start",
                "NodeSimilarity :: compare node pairs :: Finished",
                "NodeSimilarity :: Finished"
            );
    }

    @ParameterizedTest(name = "concurrency = {0}")
    @ValueSource(ints = {1, 2})
    void shouldLogProgress(int concurrency) {
        var graph = naturalGraph;
        var config = ImmutableNodeSimilarityStreamConfig.builder().degreeCutoff(0).concurrency(concurrency).build();
        var progressTask = new NodeSimilarityFactory<>().progressTask(graph, config);
        TestLog log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            progressTask,
            log,
            concurrency,
            EmptyTaskRegistryFactory.INSTANCE
        );

        NodeSimilarity.create(
            graph,
            config,
            Pools.DEFAULT,
            progressTracker
        ).compute().streamResult().count();

        List<AtomicLong> progresses = progressTracker.getProgresses();

        // Should log progress for prepare and actual comparisons
        assertEquals(4, progresses.size());
        assertEquals(graph.relationshipCount(), progresses.get(1).get());

        assertThat(log.getMessages(INFO))
            .extracting(removingThreadId())
            .contains(
                "NodeSimilarity :: prepare :: Start",
                "NodeSimilarity :: prepare :: Finished",
                "NodeSimilarity :: compare node pairs :: Start",
                "NodeSimilarity :: compare node pairs :: Finished"
            );
    }

    @Test
    void shouldGiveCorrectResultsWithOverlap() {
        var gdl =
            "CREATE" +
            "  (a:Person)" +
            ", (c:Person)" +
            ", (i1:Item)" +
            ", (i2:Item)" +
            ", (i3:Item)" +
            ", (i4:Item)" +
            ", (a)-[:LIKES {prop: 1.0}]->(i1)" +
            ", (a)-[:LIKES {prop: 2.0}]->(i4)" +
            ", (c)-[:LIKES {prop: 3.0}]->(i3)" +
            ", (c)-[:LIKES {prop: 4.0}]->(i1)";

        Graph graph = fromGdl(gdl);

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder().concurrency(1).similarityMetric(MetricSimilarityComputer.parse("ovErLaP")).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        Set<String> result = nodeSimilarity
            .computeToStream()
            .map(NodeSimilarityTest::resultString)
            .collect(Collectors.toSet());

        assertThat(result).contains("0,1 0.500000");

        nodeSimilarity = NodeSimilarity.create(
            graph,
            configBuilder()
                .relationshipWeightProperty("LIKES")
                .concurrency(1)
                .similarityMetric(MetricSimilarityComputer.parse("ovErLaP"))
                .build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        result = nodeSimilarity.computeToStream().map(NodeSimilarityTest::resultString).collect(Collectors.toSet());

        assertThat(result).contains("0,1 0.333333");

    }

    static Stream<Arguments> degreeCutoffInput() {
        return Stream.of(
            arguments(2, Integer.MAX_VALUE,
                new String[]{resultString(0, 1, 0.666667),
                    resultString(3, 0, 1.0), resultString(3, 1, 0.666667),
                    resultString(1, 3, 0.666667), resultString(1, 0, 0.666667),
                    resultString(0, 3, 1.0)}
            ),
            arguments(1, 2,
                new String[]{resultString(1, 2, 0.0),
                    resultString(2, 1, 0.0)}  //their similarity is zero, but we still return them
            ),
            arguments(3, 3,
                new String[]{resultString(0, 3, 1.0),
                    resultString(3, 0, 1.0)}
            )
        );
    }

    @ParameterizedTest
    @MethodSource("degreeCutoffInput")
    void shouldWorkForAllDegreeBoundsCombinations(int lowBound, int upperBound, String... expectedOutput) {

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            naturalGraph,
            configBuilder().upperDegreeCutoff(upperBound).degreeCutoff(lowBound).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        Set<String> result = nodeSimilarity
            .computeToStream()
            .map(NodeSimilarityTest::resultString)
            .collect(Collectors.toSet());

        assertThat(result).containsExactlyInAnyOrder(expectedOutput);
    }

    @Test
    void shouldThrowIfUpperIsSmaller() {
        assertThatThrownBy(configBuilder().upperDegreeCutoff(3).degreeCutoff(4)::build)
            .hasMessageContaining("upperDegreeCutoff cannot be smaller than degreeCutoff");
    }

}
