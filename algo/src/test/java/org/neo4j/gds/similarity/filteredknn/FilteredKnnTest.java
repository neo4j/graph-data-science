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
package org.neo4j.gds.similarity.filteredknn;

import com.carrotsearch.hppc.LongArrayList;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.NullPropertyMap;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.nodeproperties.DoubleArrayTestPropertyValues;
import org.neo4j.gds.nodeproperties.DoubleTestPropertyValues;
import org.neo4j.gds.nodeproperties.FloatArrayTestPropertyValues;
import org.neo4j.gds.similarity.knn.KnnNodePropertySpec;
import org.neo4j.gds.similarity.knn.KnnSampler;
import org.neo4j.gds.similarity.knn.metrics.SimilarityComputer;
import org.neo4j.gds.similarity.knn.metrics.SimilarityMetric;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withPrecision;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
@ExtendWith(SoftAssertionsExtension.class)
class FilteredKnnTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a { knn: 1.2, prop: 1.0 } )" +
        ", (b { knn: 1.1, prop: 5.0 } )" +
        ", (c { knn: 42.0, prop: 10.0 } )";
    @Inject
    private Graph graph;

    @GdlGraph(graphNamePrefix = "simThreshold")
    private static final String nodeCreateQuery =
        "CREATE " +
        "  (alice:Person {age: 23})" +
        " ,(carol:Person {age: 24})" +
        " ,(eve:Person {age: 34})" +
        " ,(bob:Person {age: 30})";
    @Inject
    private TestGraph simThresholdGraph;

    @GdlGraph(graphNamePrefix = "multPropMissing")
    private static final String nodeCreateMultipleQuery =
        "CREATE " +
        "  (a: P1 {prop1: 1.0, prop2: 5.0})" +
        " ,(b: P1 {prop2 : 5.0})" +
        " ,(c: P1 {prop2 : 10.0})" +
        " ,(d: P1 {prop3 : 10.0})";
    @Inject
    private TestGraph multPropMissingGraph;


    @Inject
    private IdFunction idFunction;

    @Test
    void shouldRun() {
        var knnConfig = ImmutableFilteredKnnBaseConfig.builder()
            .nodeProperties(List.of(new KnnNodePropertySpec("knn")))
            .concurrency(1)
            .randomSeed(19L)
            .topK(1)
            .build();
        var knnContext = ImmutableFilteredKnnContext.builder().build();

        var knn = FilteredKnn.createWithDefaults(graph, knnConfig, knnContext);
        var result = knn.compute();

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(3);

        long nodeAId = idFunction.of("a");
        long nodeBId = idFunction.of("b");
        long nodeCId = idFunction.of("c");

        assertCorrectNeighborList(result, nodeAId, nodeBId);
        assertCorrectNeighborList(result, nodeBId, nodeAId);
        assertCorrectNeighborList(result, nodeCId, nodeAId);
    }

    @Test
    void shouldHaveEachNodeConnected() {
        var knnConfig = ImmutableFilteredKnnBaseConfig.builder()
            .nodeProperties(List.of(new KnnNodePropertySpec("knn")))
            .topK(2)
            .build();
        var knnContext = ImmutableFilteredKnnContext.builder().build();

        var knn = FilteredKnn.createWithDefaults(graph, knnConfig, knnContext);
        var result = knn.compute();

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(3);

        long nodeAId = idFunction.of("a");
        long nodeBId = idFunction.of("b");
        long nodeCId = idFunction.of("c");

        assertCorrectNeighborList(result, nodeAId, nodeBId, nodeCId);
        assertCorrectNeighborList(result, nodeBId, nodeAId, nodeCId);
        assertCorrectNeighborList(result, nodeCId, nodeAId, nodeBId);
    }
    private void assertCorrectNeighborList(
        FilteredKnnResult result,
        long nodeId,
        long... expectedNeighbors
    ) {
        var actualSimilarityPairs = result.neighborList().get(nodeId).similarityStream(nodeId);
        var actualNeighbors = result.neighborsOf(nodeId).toArray();
        assertThat(actualNeighbors)
            .doesNotContain(nodeId)
            .containsAnyOf(expectedNeighbors)
            .doesNotHaveDuplicates()
            .hasSizeLessThanOrEqualTo(expectedNeighbors.length);
        assertThat(actualSimilarityPairs)
            .isSortedAccordingTo(Comparator.comparingDouble((s) -> -s.similarity));
    }

    @Test
    void shouldWorkWithMultipleProperties() {

        var knnConfig = ImmutableFilteredKnnBaseConfig.builder()
            .nodeProperties(List.of(new KnnNodePropertySpec("knn"), new KnnNodePropertySpec("prop")))
            .concurrency(1)
            .randomSeed(19L)
            .topK(1)
            .build();
        var knnContext = ImmutableFilteredKnnContext.builder().build();

        var knn = FilteredKnn.createWithDefaults(graph, knnConfig, knnContext);
        var result = knn.compute();

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(3);
        long nodeAId = idFunction.of("a");
        long nodeBId = idFunction.of("b");
        long nodeCId = idFunction.of("c");

        double EXP_A = 0.5 * (1 / 1.1) + 0.5 * (1 / 5.0);
        double EXP_B = 0.5 * (1 / 1.1) + 0.5 * (1 / 5.0);
        double EXP_C = 0.5 * (1 / 41.8) + 0.5 * (1 / 6.0);

        assertCorrectNeighborList(result, nodeAId, nodeBId);
        assertCorrectNeighborList(result, nodeBId, nodeAId);
        assertCorrectNeighborList(result, nodeCId, nodeBId);
        assertThat(result.neighborList().get(nodeAId).similarityStream(nodeAId).findFirst().get().similarity).isEqualTo(
            EXP_A, withPrecision(0.001));
        assertThat(result.neighborList().get(nodeBId).similarityStream(nodeBId).findFirst().get().similarity).isEqualTo(
            EXP_B, withPrecision(0.001));
        assertThat(result.neighborList().get(nodeCId).similarityStream(nodeCId).findFirst().get().similarity).isEqualTo(
            EXP_C, withPrecision(0.001));


    }

    @Test
    void shouldWorkWithMultiplePropertiesEvenIfSomeAreMissing() {

        var knnConfig = ImmutableFilteredKnnBaseConfig.builder()
            .nodeProperties(List.of(new KnnNodePropertySpec("prop1"), new KnnNodePropertySpec("prop2")))
            .concurrency(1)
            .randomSeed(19L)
            .topK(2)
            .build();
        var knnContext = ImmutableFilteredKnnContext.builder().build();

        var knn = FilteredKnn.createWithDefaults(multPropMissingGraph, knnConfig, knnContext);
        var result = knn.compute();

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(4);

        long nodeAId = multPropMissingGraph.toMappedNodeId("a");
        long nodeBId = multPropMissingGraph.toMappedNodeId("b");
        long nodeCId = multPropMissingGraph.toMappedNodeId("c");
        long nodeDId = multPropMissingGraph.toMappedNodeId("d");


        assertCorrectNeighborList(result, nodeAId, nodeBId, nodeCId);
        assertCorrectNeighborList(result, nodeBId, nodeAId, nodeCId);
        assertThat(result.neighborList().get(nodeAId).similarityStream(nodeAId).findFirst().get().similarity).isEqualTo(
            0.5, withPrecision(0.1));
        assertThat(result.neighborList().get(nodeCId).similarityStream(nodeCId).findFirst().get().similarity).isEqualTo(
            0.083, withPrecision(0.01));
        assertThat(result.neighborList().get(nodeDId).similarityStream(nodeDId).findFirst().get().similarity).isEqualTo(
            0.0, withPrecision(0.001));

    }

    @Test
    void shouldFilterResultsOfLowSimilarity() {

        var knnConfig = ImmutableFilteredKnnBaseConfig.builder()
            .nodeProperties(List.of(new KnnNodePropertySpec("age")))
            .concurrency(1)
            .randomSeed(19L)
            .similarityCutoff(0.14)
            .topK(2)
            .build();
        var knnContext = ImmutableFilteredKnnContext.builder().build();

        var knn = FilteredKnn.createWithDefaults(simThresholdGraph, knnConfig, knnContext);
        var result = knn.compute();

        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(4);

        long nodeAliceId = simThresholdGraph.toMappedNodeId("alice");
        long nodeBobId = simThresholdGraph.toMappedNodeId("bob");
        long nodeEveId = simThresholdGraph.toMappedNodeId("eve");
        long nodeCarolId = simThresholdGraph.toMappedNodeId("carol");

        assertCorrectNeighborList(result, nodeAliceId, nodeCarolId);
        assertCorrectNeighborList(result, nodeCarolId, nodeAliceId, nodeBobId);
        assertCorrectNeighborList(result, nodeBobId, nodeEveId, nodeCarolId);
        assertCorrectNeighborList(result, nodeEveId, nodeBobId);
    }

    private void assertEmptyNeighborList(FilteredKnnResult result, long nodeId) {
        var actualNeighbors = result.neighborsOf(nodeId).toArray();
        assertThat(actualNeighbors).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("emptyProperties")
    void testNonExistingProperties(NodePropertyValues nodeProperties) {
        var knnConfig = ImmutableFilteredKnnBaseConfig.builder()
            .nodeProperties(List.of(new KnnNodePropertySpec("knn")))
            .topK(2)
            .build();
        var knnContext = ImmutableFilteredKnnContext.builder().build();
        var knn = FilteredKnn.create(
            graph,
            knnConfig,
            knnContext,
            SimilarityComputer.ofProperty(graph, "knn", nodeProperties),
            new FilteredKnnNeighborFilterFactory(graph.nodeCount())
        );
        var result = knn.compute();
        assertThat(result)
            .isNotNull()
            .extracting(FilteredKnnResult::size)
            .isEqualTo(3L);
    }

    static Stream<NodePropertyValues> emptyProperties() {
        return Stream.of(
            new DoubleTestPropertyValues(nodeId -> Double.NaN),
            new NullPropertyMap.DoubleNullPropertyMap(Double.NaN),
            new FloatArrayTestPropertyValues(nodeId -> new float[]{}),
            new DoubleArrayTestPropertyValues(nodeId -> new double[]{})
        );
    }

    @Test
    void testMixedExistingAndNonExistingProperties(SoftAssertions softly) {
        var nodeProperties = new DoubleTestPropertyValues(nodeId -> nodeId == 0 ? Double.NaN : 42.1337);
        var knn = FilteredKnn.create(
            graph,
            ImmutableFilteredKnnBaseConfig
                .builder()
                .nodeProperties(List.of(new KnnNodePropertySpec("knn")))
                .topK(1)
                .concurrency(1)
                .randomSeed(42L)
                .build(),
            ImmutableFilteredKnnContext.builder().build(),
            SimilarityComputer.ofProperty(graph, "{knn}", nodeProperties),
            new FilteredKnnNeighborFilterFactory(graph.nodeCount())
        );

        var result = knn.compute();

        softly.assertThat(result)
            .isNotNull()
            .extracting(FilteredKnnResult::size)
            .isEqualTo(3L);

        long nodeAId = idFunction.of("a");
        long nodeBId = idFunction.of("b");
        long nodeCId = idFunction.of("c");

        softly.assertThat(result.neighborsOf(nodeBId)).doesNotContain(nodeAId);
        softly.assertThat(result.neighborsOf(nodeCId)).doesNotContain(nodeAId);
    }

    @Test
    void testReverseEmptyList() {
        var nodeCount = 42;

        var neighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);
        var reverseNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);

        // no old elements, don't add something to the reverse neighbors
        FilteredKnn.reverseNeighbors(0, neighbors, reverseNeighbors);
        assertThat(reverseNeighbors.get(0)).isNull();

        var neighborsFrom0 = LongArrayList.from(LongStream.range(1, nodeCount).toArray());
        neighbors.set(0, neighborsFrom0);
    }

    @Test
    void testReverseAllAsNeighbor() {
        var nodeCount = 42;

        var neighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);
        var reverseNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);

        // 0 is neighboring every other node
        var neighborsFrom0 = LongArrayList.from(LongStream.range(1, nodeCount).toArray());
        neighbors.set(0, neighborsFrom0);

        FilteredKnn.reverseNeighbors(0, neighbors, reverseNeighbors);
        // 0 has no reverse neighbors
        assertThat(reverseNeighbors.get(0)).isNull();
        // every other node points to 0
        for (int i = 1; i < nodeCount; i++) {
            var reversed = reverseNeighbors.get(i);
            assertThat(reversed)
                .isNotNull()
                .singleElement()
                // list returns hppc cursor, access value field
                .extracting("value")
                .isEqualTo(0L);
        }

    }

    @Test
    void testReverseSingleNeighbors() {
        var nodeCount = 42;

        var neighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);
        var reverseNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);

        // every node other than 0 has 0 as neighbor
        neighbors.setAll(nodeId -> nodeId == 0 ? null : LongArrayList.from(0));

        for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
            FilteredKnn.reverseNeighbors(nodeId, neighbors, reverseNeighbors);
        }

        // all nodes point to 0
        assertThat(reverseNeighbors.get(0))
            .isNotNull()
            .extracting(c -> c.value)
            .containsExactly(LongStream.range(1, nodeCount).boxed().toArray(Long[]::new));

        // all other nodes have no reverse neighbors
        for (int i = 1; i < nodeCount; i++) {
            assertThat(reverseNeighbors.get(i)).isNull();
        }
    }

    @Test
    void testNegativeFloatArrays() {
        var graph = GdlFactory.of("({weight: [1.0, 2.0]}), ({weight: [3.0, -10.0]})").build().getUnion();

        var knnConfig = ImmutableFilteredKnnBaseConfig.builder()
            .nodeProperties(List.of(new KnnNodePropertySpec("weight")))
            .topK(1)
            .build();
        var knnContext = ImmutableFilteredKnnContext.builder().build();

        var knn = FilteredKnn.createWithDefaults(graph, knnConfig, knnContext);

        var result = knn.compute();

        assertThat(result.neighborsOf(0)).containsExactly(1L);
        assertThat(result.neighborsOf(1)).containsExactly(0L);
    }

    @Test
    void shouldLogProgress() {
        var config = ImmutableFilteredKnnBaseConfig.builder()
            .nodeProperties(List.of(new KnnNodePropertySpec("knn")))
            .randomSeed(42L)
            .topK(1)
            .concurrency(1)
            .build();

        var factory = new FilteredKnnFactory<>();

        var progressTask = factory.progressTask(graph, config);
        var log = Neo4jProxy.testLog();
        var progressTracker = new TaskProgressTracker(progressTask, log, 4, EmptyTaskRegistryFactory.INSTANCE);

        factory
            .build(graph, config, progressTracker)
            .compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "FilteredKnn :: Start",
                "FilteredKnn :: Initialize random neighbors :: Start",
                "FilteredKnn :: Initialize random neighbors 100%",
                "FilteredKnn :: Initialize random neighbors :: Finished",
                "FilteredKnn :: Graph init took `some time`",
                "FilteredKnn :: Iteration :: Start",
                "FilteredKnn :: Iteration :: Split old and new neighbors 1 of 100 :: Start",
                "FilteredKnn :: Iteration :: Split old and new neighbors 1 of 100 100%",
                "FilteredKnn :: Iteration :: Split old and new neighbors 1 of 100 :: Finished",
                "FilteredKnn :: Iteration :: Reverse old and new neighbors 1 of 100 :: Start",
                "FilteredKnn :: Iteration :: Reverse old and new neighbors 1 of 100 100%",
                "FilteredKnn :: Iteration :: Reverse old and new neighbors 1 of 100 :: Finished",
                "FilteredKnn :: Iteration :: Join neighbors 1 of 100 :: Start",
                "FilteredKnn :: Iteration :: Join neighbors 1 of 100 100%",
                "FilteredKnn :: Iteration :: Join neighbors 1 of 100 :: Finished",
                "FilteredKnn :: Iteration :: Graph iteration 1 took `some time`",
                "FilteredKnn :: Iteration :: Finished",
                "FilteredKnn :: Finished",
                "FilteredKnn :: Graph execution took `some time`"
            );
    }

    @Test
    void shouldRenderNodePropertiesWithResolvedDefaultMetrics() {
        var userInput = CypherMapWrapper.create(
            Map.of(
                "nodeProperties", List.of("knn")
            )
        );
        var knnConfig = new FilteredKnnBaseConfigImpl(userInput);
        var knnContext = ImmutableFilteredKnnContext.builder().build();

        // Initializing KNN will cause the default metric to be resolved
        FilteredKnn.createWithDefaults(graph, knnConfig, knnContext);

        assertThat(knnConfig.toMap().get("nodeProperties")).isEqualTo(
            Map.of(
                "knn", SimilarityMetric.DOUBLE_PROPERTY_METRIC.name()
            )
        );
    }

    @Test
    void invalidRandomParameters() {
        var configBuilder = ImmutableFilteredKnnBaseConfig.builder()
            .nodeProperties(List.of(new KnnNodePropertySpec("dummy")))
            .concurrency(4)
            .randomSeed(1337L);
        assertThrows(IllegalArgumentException.class, configBuilder::build);
    }

    @Nested
    class IterationsLimitTest {

        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a { knn: 1.2 } )" +
            ", (b { knn: 1.1 } )" +
            ", (c { knn: 2.1 } )" +
            ", (d { knn: 3.1 } )" +
            ", (e { knn: 4.1 } )" +
            ", (f { knn: 5.1 } )" +
            ", (g { knn: 6.1 } )" +
            ", (h { knn: 7.1 } )" +
            ", (j { knn: 42.0 } )";

        @Test
        void shouldRespectIterationLimit() {
            var config = ImmutableFilteredKnnBaseConfig.builder()
                .nodeProperties(List.of(new KnnNodePropertySpec("knn")))
                .deltaThreshold(0)
                .topK(1)
                .maxIterations(1)
                .randomSeed(42L)
                .concurrency(1)
                .build();
            var knnContext = FilteredKnnContext.empty();
            var knn = FilteredKnn.createWithDefaults(graph, config, knnContext);
            var result = knn.compute();

            assertEquals(1, result.ranIterations());
            assertFalse(result.didConverge());
        }

    }

    @Nested
    class DidConvergeTest {

        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a { knn: 1.0 } )" +
            ", (b { knn: 1.0 } )";

        @Test
        void shouldReturnCorrectNumberIterationsWhenConverging() {
            var config = ImmutableFilteredKnnBaseConfig.builder()
                .nodeProperties(List.of(new KnnNodePropertySpec("knn")))
                .deltaThreshold(1.0)
                .maxIterations(5)
                .build();

            var knnContext = FilteredKnnContext.empty();
            var knn = FilteredKnn.createWithDefaults(graph, config, knnContext);
            var result = knn.compute();

            assertTrue(result.didConverge());
            assertEquals(1, result.ranIterations());
        }

    }

    @Nested
    class RandomWalkInitialSamplerTest {

        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a { knn: 1.2 } )" +
            ", (b { knn: 1.1 } )" +
            ", (c { knn: 2.1 } )" +
            ", (d { knn: 3.1 } )" +
            ", (e { knn: 4.1 } )" +
            ", (f { knn: 5.1 } )" +
            ", (g { knn: 6.1 } )" +

            ", (a)-[:TYPE1]->(b)" +
            ", (a)-[:TYPE1]->(d)" +
            ", (b)-[:TYPE1]->(d)" +
            ", (b)-[:TYPE1]->(e)" +
            ", (b)-[:TYPE1]->(f)" +
            ", (b)-[:TYPE1]->(g)" +
            ", (c)-[:TYPE1]->(b)" +
            ", (c)-[:TYPE1]->(e)" +
            ", (d)-[:TYPE1]->(c)" +
            ", (d)-[:TYPE1]->(b)" +
            ", (e)-[:TYPE1]->(b)" +
            ", (f)-[:TYPE1]->(a)" +
            ", (f)-[:TYPE1]->(b)" +
            ", (g)-[:TYPE1]->(b)" +
            ", (g)-[:TYPE1]->(c)" +
            ", (g)-[:TYPE1]->(g)";

        @Test
        void testReasonableTopKWithRandomWalk(SoftAssertions softly) {
            var config = ImmutableFilteredKnnBaseConfig.builder()
                .nodeProperties(List.of(new KnnNodePropertySpec("knn")))
                .topK(4)
                .randomJoins(0)
                .maxIterations(1)
                .randomSeed(20L)
                .concurrency(1)
                .initialSampler(KnnSampler.SamplerType.RANDOMWALK)
                .build();
            var knnContext = FilteredKnnContext.empty();
            var knn = FilteredKnn.createWithDefaults(graph, config, knnContext);
            var result = knn.compute();

            long nodeAId = idFunction.of("a");
            long nodeBId = idFunction.of("b");
            long nodeCId = idFunction.of("c");
            long nodeDId = idFunction.of("d");
            long nodeEId = idFunction.of("e");
            long nodeFId = idFunction.of("f");
            long nodeGId = idFunction.of("g");

            softly.assertThat(result.neighborsOf(nodeAId)).contains(nodeBId);
            softly.assertThat(result.neighborsOf(nodeBId)).contains(nodeAId);
            softly.assertThat(result.neighborsOf(nodeCId)).contains(nodeBId);
            softly.assertThat(result.neighborsOf(nodeDId)).contains(nodeEId);
            softly.assertThat(result.neighborsOf(nodeEId)).contains(nodeFId);
            softly.assertThat(result.neighborsOf(nodeFId)).contains(nodeGId);
            softly.assertThat(result.neighborsOf(nodeGId)).contains(nodeFId);
        }
    }

    @Nested
    class SourceNodeFilterTest {

        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a { knn: 1.2 } )" +
            ", (b { knn: 1.1 } )" +
            ", (c { knn: 2.1 } )" +
            ", (d { knn: 3.1 } )" +
            ", (e { knn: 4.1 } )";

        @Test
        void shouldOnlyProduceResultsForFilteredSourceNode() {
            var filteredSourceNode = idFunction.of("a");
            var config = ImmutableFilteredKnnBaseConfig.builder()
                .nodeProperties(List.of(new KnnNodePropertySpec("knn")))
                .topK(3)
                .randomJoins(0)
                .maxIterations(1)
                .randomSeed(20L)
                .concurrency(1)
                .sourceNodeFilter(List.of(filteredSourceNode))
                .build();
            var knnContext = FilteredKnnContext.empty();
            var knn = FilteredKnn.createWithDefaults(graph, config, knnContext);
            var result = knn.compute();

            // the number of neighbors for the filtered node is the same as the total result size
            assertThat(result.neighborsOf(filteredSourceNode).count())
                .isEqualTo(result.totalSimilarityPairs());
        }

        @Test
        void shouldOnlyProduceResultsForMultipleFilteredSourceNode() {
            var filteredNode1 = idFunction.of("a");
            var filteredNode2 = idFunction.of("b");
            var config = ImmutableFilteredKnnBaseConfig.builder()
                .nodeProperties(List.of(new KnnNodePropertySpec("knn")))
                .topK(3)
                .randomJoins(0)
                .maxIterations(1)
                .randomSeed(20L)
                .concurrency(1)
                .sourceNodeFilter(List.of(filteredNode1, filteredNode2))
                .build();
            var knnContext = FilteredKnnContext.empty();
            var knn = FilteredKnn.createWithDefaults(graph, config, knnContext);
            var result = knn.compute();

            // the number of neighbors for the filtered nodes is the same as the total result size
            var neighborCountForFilteredNodes = result.neighborsOf(filteredNode1).count() + result.neighborsOf(filteredNode2).count();
            var totalNeighborCount = result.totalSimilarityPairs();
            assertThat(neighborCountForFilteredNodes).isEqualTo(totalNeighborCount);
        }

    }
}
