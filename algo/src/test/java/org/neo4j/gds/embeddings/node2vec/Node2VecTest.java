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
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.gdl.GdlFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
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
        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(a)" +
        ", (a)-[:REL]->(c)" +
        ", (c)-[:REL]->(a)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(b)";

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
        HugeObjectArray<Vector> node2Vec = new Node2Vec(
            graph,
            ImmutableNode2VecStreamConfig.builder().embeddingDimension(embeddingDimension).build(),
            progressLogger,
            AllocationTracker.empty()
        ).compute();

        graph.forEachNode(node -> {
                assertEquals(embeddingDimension, node2Vec.get(node).data().length);
                return true;
            }
        );
    }

    @Test
    void shouldLogProgress() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .build()
            .graph();

        int embeddingDimension = 128;
        Node2VecStreamConfig config = ImmutableNode2VecStreamConfig.builder().embeddingDimension(embeddingDimension).build();
        var testLogger = new TestProgressLogger(
            graph.nodeCount() * config.walksPerNode(),
            "Node2Vec",
            4
        );
        new Node2Vec(
            graph,
            config,
            testLogger,
            AllocationTracker.empty()
        ).compute();

        List<AtomicLong> progresses = testLogger.getProgresses();

        // We "reset" the logger once per iteration
        assertEquals(config.iterations() + 1, progresses.size());
        progresses.forEach(progress -> assertTrue(progress.get() <= graph.nodeCount() * config.walksPerNode()));

        assertTrue(testLogger.containsMessage(TestLog.INFO, ":: Start"));
        assertTrue(testLogger.containsMessage(TestLog.INFO, ":: Finished"));
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

        var node2Vec = new Node2Vec(graph, config, ProgressLogger.NULL_LOGGER, AllocationTracker.empty());

        assertThatThrownBy(node2Vec::compute)
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Found an invalid relationship between 0 and 1 with the property value of -1.000000." +
                        " Node2Vec only supports non-negative weights.");

    }

    static Stream<Arguments> graphs() {
        return Stream.of(
            Arguments.of("All Labels", List.of()),
            Arguments.of("Non Consecutive Original IDs", List.of("Node2", "Isolated"))
        );
    }
}
