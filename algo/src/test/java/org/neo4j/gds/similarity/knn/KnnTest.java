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
package org.neo4j.gds.similarity.knn;

import com.carrotsearch.hppc.LongArrayList;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.loading.NullPropertyMap;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.nodeproperties.DoubleArrayTestProperties;
import org.neo4j.gds.nodeproperties.DoubleTestProperties;
import org.neo4j.gds.nodeproperties.FloatArrayTestProperties;

import java.util.Comparator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
@ExtendWith(SoftAssertionsExtension.class)
class KnnTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a { knn: 1.2 } )" +
        ", (b { knn: 1.1 } )" +
        ", (c { knn: 42.0 } )";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldRun() {
        var knnConfig = ImmutableKnnBaseConfig.builder()
            .nodeWeightProperty("knn")
            .topK(1)
            .build();
        var knnContext = ImmutableKnnContext.builder().build();

        var knn = new Knn(graph, knnConfig, knnContext);
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
        var knnConfig = ImmutableKnnBaseConfig.builder()
            .nodeWeightProperty("knn")
            .topK(2)
            .build();
        var knnContext = ImmutableKnnContext.builder().build();

        var knn = new Knn(graph, knnConfig, knnContext);
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
        Knn.Result result,
        long nodeId,
        long... expectedNeighbors
    ) {
        var actualNeighbors = result.neighborsOf(nodeId).toArray();
        assertThat(actualNeighbors)
            .doesNotContain(nodeId)
            .containsAnyOf(expectedNeighbors)
            .doesNotHaveDuplicates()
            .isSortedAccordingTo(Comparator.naturalOrder())
            .hasSizeLessThanOrEqualTo(expectedNeighbors.length);
    }

    @ParameterizedTest
    @MethodSource("emptyProperties")
    void testNonExistingProperties(NodeProperties nodeProperties) {
        var knnConfig = ImmutableKnnBaseConfig.builder()
            .nodeWeightProperty("knn")
            .topK(2)
            .build();
        var knnContext = ImmutableKnnContext.builder().build();
        var knn = new Knn(
            graph.nodeCount(),
            knnConfig,
            SimilarityComputer.ofProperty(nodeProperties, "knn"),
            knnContext
        );
        var result = knn.compute();

        assertThat(result)
            .isNotNull()
            .extracting(Knn.Result::size)
            .isEqualTo(3L);
    }

    static Stream<NodeProperties> emptyProperties() {
        return Stream.of(
            new DoubleTestProperties(nodeId -> Double.NaN),
            new NullPropertyMap.DoubleNullPropertyMap(Double.NaN),
            new FloatArrayTestProperties(nodeId -> new float[]{}),
            new DoubleArrayTestProperties(nodeId -> new double[]{})
        );
    }

    @Test
    void testMixedExistingAndNonExistingProperties(SoftAssertions softly) {
        var nodeProperties = new DoubleTestProperties(nodeId -> nodeId == 0 ? Double.NaN : 42.1337);
        var knn = new Knn(
            graph.nodeCount(),
            ImmutableKnnBaseConfig.builder().nodeWeightProperty("knn").topK(1).randomSeed(42L).build(),
            SimilarityComputer.ofProperty(nodeProperties, "knn"),
            ImmutableKnnContext.builder().build()
        );

        var result = knn.compute();

        softly.assertThat(result)
            .isNotNull()
            .extracting(Knn.Result::size)
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

        var neighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount, AllocationTracker.empty());
        var reverseNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount, AllocationTracker.empty());

        // no old elements, don't add something to the reverse neighbors
        Knn.reverseNeighbors(0, neighbors, reverseNeighbors);
        assertThat(reverseNeighbors.get(0)).isNull();

        var neighborsFrom0 = LongArrayList.from(LongStream.range(1, nodeCount).toArray());
        neighbors.set(0, neighborsFrom0);
    }

    @Test
    void testReverseAllAsNeighbor() {
        var nodeCount = 42;

        var neighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount, AllocationTracker.empty());
        var reverseNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount, AllocationTracker.empty());

        // 0 is neighboring every other node
        var neighborsFrom0 = LongArrayList.from(LongStream.range(1, nodeCount).toArray());
        neighbors.set(0, neighborsFrom0);

        Knn.reverseNeighbors(0, neighbors, reverseNeighbors);
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

        var neighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount, AllocationTracker.empty());
        var reverseNeighbors = HugeObjectArray.newArray(LongArrayList.class, nodeCount, AllocationTracker.empty());

        // every node other than 0 has 0 as neighbor
        neighbors.setAll(nodeId -> nodeId == 0 ? null : LongArrayList.from(0));

        for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
            Knn.reverseNeighbors(nodeId, neighbors, reverseNeighbors);
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
        var graph = GdlFactory.of("({weight: [1.0, 2.0]}), ({weight: [3.0, -10.0]})").build().graphStore().getUnion();

        var knnConfig = ImmutableKnnBaseConfig.builder()
            .nodeWeightProperty("weight")
            .topK(1)
            .build();
        var knnContext = ImmutableKnnContext.builder().build();

        var knn = new Knn(graph, knnConfig, knnContext);

        var result = knn.compute();

        assertThat(result.neighborsOf(0)).containsExactly(1L);
        assertThat(result.neighborsOf(1)).containsExactly(0L);
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
            var config = ImmutableKnnBaseConfig.builder()
                .nodeWeightProperty("knn")
                .deltaThreshold(0)
                .maxIterations(1)
                .build();
            var knnContext = KnnContext.empty();
            var knn = new Knn(graph, config, knnContext);
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
            var config = ImmutableKnnBaseConfig.builder()
                .nodeWeightProperty("knn")
                .deltaThreshold(1.0)
                .maxIterations(5)
                .build();

            var knnContext = KnnContext.empty();
            var knn = new Knn(graph, config, knnContext);
            var result = knn.compute();

            assertTrue(result.didConverge());
            assertEquals(1, result.ranIterations());
        }

    }
}
