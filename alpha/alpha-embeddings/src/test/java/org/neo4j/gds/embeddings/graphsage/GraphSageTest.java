/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.proc.GraphSageAlgorithmFactory;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.logging.NullLog;

import java.util.Random;
import java.util.stream.LongStream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.graphalgo.TestLog.INFO;

class GraphSageTest {

    private static final int NODE_COUNT = 20;
    private static final int FEATURES_COUNT = 1;
    private static final int EMBEDDING_DIMENSION = 64;
    private static final String MODEL_NAME = "graphSageModel";

    private Graph graph;
    private HugeObjectArray<double[]> features;
    private ImmutableGraphSageTrainConfig.Builder configBuilder;

    @BeforeEach
    void setUp() {
        RandomGraphGenerator randomGraphGenerator = RandomGraphGenerator.builder()
            .nodeCount(NODE_COUNT)
            .averageDegree(3)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .seed(123L)
            .aggregation(Aggregation.SINGLE)
            .orientation(Orientation.UNDIRECTED)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .allocationTracker(AllocationTracker.empty())
            .build();
        graph = randomGraphGenerator.generate();

        long nodeCount = graph.nodeCount();
        features = HugeObjectArray.newArray(double[].class, nodeCount, AllocationTracker.empty());

        Random random = new Random();
        LongStream.range(0, nodeCount).forEach(n -> features.set(n, random.doubles(FEATURES_COUNT).toArray()));

        configBuilder = ImmutableGraphSageTrainConfig.builder()
            .degreeAsProperty(true)
            .embeddingDimension(EMBEDDING_DIMENSION);
    }

    @Test
    void testLogging() {
        var trainConfig = configBuilder
            .modelName(MODEL_NAME)
            .build();

        var modelTrainer = new GraphSageModelTrainer(trainConfig, ProgressLogger.NULL_LOGGER);
        var layers = modelTrainer.train(graph, features).layers();
        var model = Model.of(
            "",
            MODEL_NAME,
            "graphSage",
            GraphSchema.empty(),
            layers,
            trainConfig
        );
        ModelCatalog.set(model);

        var streamConfig = ImmutableGraphSageStreamConfig
            .builder()
            .modelName(MODEL_NAME)
            .batchSize(1)
            .build();

        var algorithmFactory = new GraphSageAlgorithmFactory<>(TestProgressLogger.FACTORY);
        var graphSage = algorithmFactory.build(graph, streamConfig, AllocationTracker.empty(), NullLog.getInstance());
        graphSage.compute();

        var testLogger = (TestProgressLogger) graphSage.getProgressLogger();
        var messagesInOrder = testLogger.getMessages(INFO);

        assertThat(messagesInOrder)
            // avoid asserting on the thread id
            .extracting(message -> message.substring(message.indexOf("] ") + 2))
            .containsExactly(
                "GraphSage :: Start",
                "GraphSage 5%",
                "GraphSage 10%",
                "GraphSage 15%",
                "GraphSage 20%",
                "GraphSage 25%",
                "GraphSage 30%",
                "GraphSage 35%",
                "GraphSage 40%",
                "GraphSage 45%",
                "GraphSage 50%",
                "GraphSage 55%",
                "GraphSage 60%",
                "GraphSage 65%",
                "GraphSage 70%",
                "GraphSage 75%",
                "GraphSage 80%",
                "GraphSage 85%",
                "GraphSage 90%",
                "GraphSage 95%",
                "GraphSage 100%",
                "GraphSage :: Finished"
            );
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }
}
