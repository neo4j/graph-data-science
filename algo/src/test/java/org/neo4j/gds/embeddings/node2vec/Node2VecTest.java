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
package org.neo4j.gds.embeddings.node2vec;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoTestBase;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.TestLog;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.ml.core.tensor.FloatVector;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Node2VecTest extends AlgoTestBase {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node1)" +
        ", (b:Node1)" +
        ", (c:Node2)" +
        ", (d:Isolated)" +
        ", (e:Isolated)" +
        ", (a)-[:REL {prop: 1.0}]->(b)" +
        ", (b)-[:REL {prop: 1.0}]->(a)" +
        ", (a)-[:REL {prop: 1.0}]->(c)" +
        ", (c)-[:REL {prop: 1.0}]->(a)" +
        ", (b)-[:REL {prop: 1.0}]->(c)" +
        ", (c)-[:REL {prop: 1.0}]->(b)";

    @BeforeEach
    void setUp() {
        runQuery(DB_CYPHER);
    }

    @ParameterizedTest (name = "{0}")
    @MethodSource("graphs")
    void embeddingsShouldHaveTheConfiguredDimension(String msg, Iterable<String> nodeLabels) {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .nodeLabels(nodeLabels)
            .build()
            .graph();

        int embeddingDimension = 128;
        HugeObjectArray<FloatVector> node2Vec = new Node2Vec(
            graph,
            ImmutableNode2VecStreamConfig.builder().embeddingDimension(embeddingDimension).build(),
            ProgressTracker.NULL_TRACKER,
            AllocationTracker.empty()
        ).compute();

        graph.forEachNode(node -> {
                assertEquals(embeddingDimension, node2Vec.get(node).data().length);
                return true;
            }
        );
    }

    @ParameterizedTest
    @CsvSource(value = {
        "true,4",
        "false,3"
    })
    void shouldLogProgress(boolean relationshipWeights, int expectedProgresses) {
        var storeLoaderBuilder = new StoreLoaderBuilder()
            .api(db);
        if (relationshipWeights) {
            storeLoaderBuilder.addRelationshipProperty(PropertyMapping.of("prop"));
        }
        Graph graph = storeLoaderBuilder.build().graph();

        int embeddingDimension = 128;
        Node2VecStreamConfig config = ImmutableNode2VecStreamConfig.builder().embeddingDimension(embeddingDimension).build();
        var progressTask = new Node2VecAlgorithmFactory<>().progressTask(graph, config);
        var log = new TestLog();
        var progressTracker = new TestProgressTracker(progressTask, log, 4, EmptyTaskRegistryFactory.INSTANCE);
        new Node2Vec(
            graph,
            config,
            progressTracker,
            AllocationTracker.empty()
        ).compute();

        assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: Start"));
        assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: RandomWalk :: Start"));

        if (relationshipWeights) {
            assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: RandomWalk :: DegreeCentrality :: Start"));
            assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: RandomWalk :: DegreeCentrality :: Finished"));
        }

        assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: RandomWalk :: create walks :: Start"));
        assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: RandomWalk :: create walks 100%"));
        assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: RandomWalk :: create walks :: Finished"));
        assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: RandomWalk :: Finished"));

        assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: train :: Start"));
        assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: train :: iteration 1 of 1 :: Start"));
        assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: train :: iteration 1 of 1 100%"));
        assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: train :: iteration 1 of 1 :: Finished"));
        assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: train :: Finished"));

        assertTrue(log.containsMessage(TestLog.INFO, "Node2Vec :: Finished"));
    }

    @Test
    void shouldEstimateMemory() {
        var nodeCount = 1000;
        var config = ImmutableNode2VecStreamConfig.builder().build();
        var memoryEstimation = Node2Vec.memoryEstimation(config);

        var numberOfRandomWalks = nodeCount * config.walksPerNode() * config.walkLength();
        var randomWalkMemoryUsageLowerBound = numberOfRandomWalks * Long.BYTES;

        var estimate = memoryEstimation.estimate(GraphDimensions.of(nodeCount), 1);
        assertThat(estimate.memoryUsage().max).isCloseTo(randomWalkMemoryUsageLowerBound, Percentage.withPercentage(25));

        var estimateTimesHundred = memoryEstimation.estimate(GraphDimensions.of(nodeCount * 100), 1);
        assertThat(estimateTimesHundred.memoryUsage().max).isCloseTo(randomWalkMemoryUsageLowerBound * 100L, Percentage.withPercentage(25));
    }

    @Test
    void failOnNegativeWeights() {
        var graph = GdlFactory.of("CREATE (a)-[:REL {weight: -1}]->(b)")
            .build()
            .graphStore()
            .getUnion();

        var config = ImmutableNode2VecStreamConfig
            .builder()
            .relationshipWeightProperty("weight")
            .build();

        var node2Vec = new Node2Vec(graph, config, ProgressTracker.NULL_TRACKER, AllocationTracker.empty());

        assertThatThrownBy(node2Vec::compute)
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Found an invalid relationship weight between nodes `0` and `1` with the property value of `-1.000000`." +
                        " Node2Vec only supports non-negative weights.");

    }

    static Stream<Arguments> graphs() {
        return Stream.of(
            Arguments.of("All Labels", List.of()),
            Arguments.of("Non Consecutive Original IDs", List.of("Node2", "Isolated"))
        );
    }
}
