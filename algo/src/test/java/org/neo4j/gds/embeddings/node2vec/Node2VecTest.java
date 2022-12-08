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

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.assertj.core.data.Offset;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.HugeSparseLongArray;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.ArrayIdMap;
import org.neo4j.gds.core.loading.LabelInformationBuilders;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.embeddings.node2vec.Node2VecBaseConfig.EmbeddingInitializer;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.ml.core.tensor.FloatVector;
import org.neo4j.gds.core.utils.shuffle.ShuffleUtil;

import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

@ExtendWith(SoftAssertionsExtension.class)
class Node2VecTest extends BaseTest {

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

    @ParameterizedTest(name = "{0}")
    @MethodSource("graphs")
    void embeddingsShouldHaveTheConfiguredDimension(String msg, Iterable<String> nodeLabels) {
        Graph graph = new StoreLoaderBuilder()
            .databaseService(db)
            .nodeLabels(nodeLabels)
            .build()
            .graph();

        int embeddingDimension = 128;
        HugeObjectArray<FloatVector> node2Vec = new Node2Vec(
            graph,
            ImmutableNode2VecStreamConfig.builder().embeddingDimension(embeddingDimension).build(),
            ProgressTracker.NULL_TRACKER
        ).compute().embeddings();

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
            .databaseService(db);
        if (relationshipWeights) {
            storeLoaderBuilder.addRelationshipProperty(PropertyMapping.of("prop"));
        }
        Graph graph = storeLoaderBuilder.build().graph();

        int embeddingDimension = 128;
        Node2VecStreamConfig config = ImmutableNode2VecStreamConfig
            .builder()
            .embeddingDimension(embeddingDimension)
            .build();
        var progressTask = new Node2VecAlgorithmFactory<>().progressTask(graph, config);
        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(progressTask, log, 4, EmptyTaskRegistryFactory.INSTANCE);
        new Node2Vec(
            graph,
            config,
            progressTracker
        ).compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .contains(
                "Node2Vec :: Start",
                "Node2Vec :: RandomWalk :: Start",
                "Node2Vec :: RandomWalk :: create walks :: Start",
                "Node2Vec :: RandomWalk :: create walks 100%",
                "Node2Vec :: RandomWalk :: create walks :: Finished",
                "Node2Vec :: RandomWalk :: Finished",
                "Node2Vec :: train :: Start",
                "Node2Vec :: train :: iteration 1 of 1 :: Start",
                "Node2Vec :: train :: iteration 1 of 1 100%",
                "Node2Vec :: train :: iteration 1 of 1 :: Finished",
                "Node2Vec :: train :: Finished",
                "Node2Vec :: Finished"
            );

        if (relationshipWeights) {
            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .contains(
                    "Node2Vec :: RandomWalk :: DegreeCentrality :: Start",
                    "Node2Vec :: RandomWalk :: DegreeCentrality :: Finished"
                );
        }
    }

    @Test
    void shouldEstimateMemory() {
        var nodeCount = 1000;
        var config = ImmutableNode2VecStreamConfig.builder().build();
        var memoryEstimation = Node2Vec.memoryEstimation(config);

        var numberOfRandomWalks = nodeCount * config.walksPerNode() * config.walkLength();
        var randomWalkMemoryUsageLowerBound = numberOfRandomWalks * Long.BYTES;

        var estimate = memoryEstimation.estimate(GraphDimensions.of(nodeCount), 1);
        assertThat(estimate.memoryUsage().max).isCloseTo(
            randomWalkMemoryUsageLowerBound,
            Percentage.withPercentage(25)
        );

        var estimateTimesHundred = memoryEstimation.estimate(GraphDimensions.of(nodeCount * 100), 1);
        assertThat(estimateTimesHundred.memoryUsage().max).isCloseTo(
            randomWalkMemoryUsageLowerBound * 100L,
            Percentage.withPercentage(25)
        );
    }

    @Test
    void failOnNegativeWeights() {
        var graph = GdlFactory.of("CREATE (a)-[:REL {weight: -1}]->(b)").build().getUnion();

        var config = ImmutableNode2VecStreamConfig
            .builder()
            .relationshipWeightProperty("weight")
            .build();

        var node2Vec = new Node2Vec(graph, config, ProgressTracker.NULL_TRACKER);

        assertThatThrownBy(node2Vec::compute)
            .isInstanceOf(RuntimeException.class)
            .hasMessage(
                "Found an invalid relationship weight between nodes `0` and `1` with the property value of `-1.000000`." +
                " Node2Vec only supports non-negative weights.");

    }

    @Disabled("The order of the randomWalks + its usage in the training is not deterministic yet.")
    @Test
    void randomSeed(SoftAssertions softly) {
        Graph graph = new StoreLoaderBuilder().databaseService(db).build().graph();

        int embeddingDimension = 2;

        var config = ImmutableNode2VecStreamConfig
            .builder()
            .embeddingDimension(embeddingDimension)
            .iterations(1)
            .negativeSamplingRate(1)
            .windowSize(1)
            .walksPerNode(1)
            .walkLength(20)
            .walkBufferSize(50)
            .randomSeed(1337L)
            .build();

        var embeddings = new Node2Vec(
            graph,
            config,
            ProgressTracker.NULL_TRACKER
        ).compute().embeddings();

        var otherEmbeddings = new Node2Vec(
            graph,
            config,
            ProgressTracker.NULL_TRACKER
        ).compute().embeddings();

        for (long node = 0; node < graph.nodeCount(); node++) {
            softly.assertThat(otherEmbeddings.get(node)).isEqualTo(embeddings.get(node));
        }
    }

    static Stream<Arguments> graphs() {
        return Stream.of(
            Arguments.of("All Labels", List.of()),
            Arguments.of("Non Consecutive Original IDs", List.of("Node2", "Isolated"))
        );
    }

    // Run the algorithm with the exact same configuration on two graphs that are exactly the same, except their node id map
    // has been shuffled. The results should still be similar.
    @ParameterizedTest
    @EnumSource(value = EmbeddingInitializer.class)
    void shouldBeFairlyConsistentUnderOriginalIds(EmbeddingInitializer embeddingInitializer) {
        long nodeCount = 1000;
        int embeddingDimension = 32;
        long degree = 4;

        var firstMappedToOriginal = HugeLongArray.newArray(nodeCount);
        firstMappedToOriginal.setAll(nodeId -> nodeId);
        var firstOriginalToMappedBuilder = HugeSparseLongArray.builder(nodeCount);
        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            firstOriginalToMappedBuilder.set(nodeId, nodeId);
        }
        // We create an IdMap explicitly instead of using a NodesBuilder in order to be sure that the id maps of the
        // graphs we produce are very different.
        var firstIdMap = new ArrayIdMap(
            firstMappedToOriginal,
            firstOriginalToMappedBuilder.build(),
            LabelInformationBuilders.singleLabel(NodeLabel.of("hello")).build(nodeCount, firstMappedToOriginal::get),
            nodeCount,
            nodeCount - 1
        );
        RelationshipsBuilder firstRelationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(firstIdMap)
            .orientation(Orientation.UNDIRECTED)
            .executorService(Pools.DEFAULT)
            .build();

        var secondMappedToOriginal = HugeLongArray.newArray(nodeCount);
        secondMappedToOriginal.setAll(nodeId -> nodeId);
        var gen = ShuffleUtil.createRandomDataGenerator(Optional.of(42L));
        ShuffleUtil.shuffleArray(secondMappedToOriginal, gen);
        var secondOriginalToMappedBuilder = HugeSparseLongArray.builder(nodeCount);
        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            secondOriginalToMappedBuilder.set(secondMappedToOriginal.get(nodeId), nodeId);
        }
        var secondIdMap = new ArrayIdMap(
            secondMappedToOriginal,
            secondOriginalToMappedBuilder.build(),
            LabelInformationBuilders.singleLabel(NodeLabel.of("hello")).build(nodeCount, secondMappedToOriginal::get),
            nodeCount,
            nodeCount - 1
        );
        RelationshipsBuilder secondRelationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(secondIdMap)
            .orientation(Orientation.UNDIRECTED)
            .executorService(Pools.DEFAULT)
            .build();

        var random = new SplittableRandom(42);
        for (long sourceNodeId = 0; sourceNodeId < nodeCount; sourceNodeId++) {
            for (int j = 0; j < degree; j++) {
                long targetNodeId = random.nextLong(nodeCount);
                firstRelationshipsBuilder.add(sourceNodeId, targetNodeId);
                secondRelationshipsBuilder.add(sourceNodeId, targetNodeId);
            }
        }
        var firstRelationships = firstRelationshipsBuilder.build();
        var secondRelationships = secondRelationshipsBuilder.build();

        var firstGraph = GraphFactory.create(firstIdMap, firstRelationships);
        var secondGraph = GraphFactory.create(secondIdMap, secondRelationships);

        var config = ImmutableNode2VecStreamConfig
            .builder()
            .embeddingInitializer(embeddingInitializer)
            .embeddingDimension(embeddingDimension)
            .randomSeed(1337L)
            .concurrency(1)
            .build();

        var firstEmbeddings = new Node2Vec(
            firstGraph,
            config,
            ProgressTracker.NULL_TRACKER
        ).compute().embeddings();

        var secondEmbeddings = new Node2Vec(
            secondGraph,
            config,
            ProgressTracker.NULL_TRACKER
        ).compute().embeddings();

        double cosineSum = 0;
        for (long originalNodeId = 0; originalNodeId < nodeCount; originalNodeId++) {
            var firstVector = firstEmbeddings.get(firstGraph.toMappedNodeId(originalNodeId));
            var secondVector = secondEmbeddings.get(secondGraph.toMappedNodeId(originalNodeId));
            double cosine = Intersections.cosine(firstVector.data(), secondVector.data(), secondVector.data().length);
            cosineSum += cosine;
        }
        //There's no hard cutoff on the average cosineSim.
        //We just want to assert different randomly initialized embeddings produce 'relatively similar' embeddings.
        assertThat(cosineSum / nodeCount).isCloseTo(1, Offset.offset(0.6));
    }
}
