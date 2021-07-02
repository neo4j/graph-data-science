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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.SingleLabelGraphSageTrain;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.beta.generator.PropertyProducer;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.progress.EmptyProgressEventTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.logging.NullLog;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.LongStream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.graphalgo.TestLog.INFO;
import static org.neo4j.graphalgo.assertj.Extractors.removingThreadId;

@GdlExtension
class GraphSageTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED, graphNamePrefix = "orphan")
    private static final String ORPHAN_GRAPH = "CREATE " +
                                               "(a:P {f1: 0.0, f2: 0.0, f3: 0.0})" +
                                               ", (b:P {f1: 1.0, f2: 0.0, f3: 0.0})" +
                                               ", (c:P {f1: 1.0, f2: 0.0, f3: 0.0})" +
                                               ", (c)-[:T]->(c)";

    @Inject
    private Graph orphanGraph;

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
            .nodeLabelProducer(nodeId -> new NodeLabel[] {NodeLabel.of("P")})
            .addNodePropertyProducer(NodeLabel.of("P"), PropertyProducer.randomDouble("f1", 0, 1))
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .relationshipPropertyProducer(PropertyProducer.fixedDouble("weight", 1.0))
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
            .embeddingDimension(EMBEDDING_DIMENSION);
    }

    @ParameterizedTest
    @EnumSource
    void shouldNotMakeNanEmbeddings(Aggregator.AggregatorType aggregator) {
        var trainConfig = configBuilder
            .modelName(MODEL_NAME)
            .aggregator(aggregator)
            .activationFunction(ActivationFunction.RELU)
            .sampleSizes(List.of(75,25))
            .featureProperties(List.of("f1", "f2", "f3"))
            .concurrency(4)
            .build();

        SingleLabelGraphSageTrain trainAlgo = new SingleLabelGraphSageTrain(
            orphanGraph,
            trainConfig,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            AllocationTracker.empty()
        );
        var model = trainAlgo.compute();
        ModelCatalog.set(model);

        var streamConfig = ImmutableGraphSageStreamConfig
            .builder()
            .modelName(MODEL_NAME)
            .concurrency(4)
            .build();

        var algorithmFactory = new GraphSageAlgorithmFactory<>(TestProgressLogger.FACTORY);
        var graphSage = algorithmFactory.build(orphanGraph, streamConfig, AllocationTracker.empty(), NullLog.getInstance(), EmptyProgressEventTracker.INSTANCE);
        GraphSage.GraphSageResult compute = graphSage.compute();
        for (int i = 0; i < orphanGraph.nodeCount() - 1; i++) {
            Arrays.stream(compute.embeddings().get(i)).forEach(embeddingValue -> {
                assertThat(embeddingValue).isNotNaN();
            });
        }
    }

    @Test
    void differentTrainAndPredictionGraph() {
        var trainConfig = configBuilder
            .modelName(MODEL_NAME)
            .featureProperties(List.of("f1"))
            .relationshipWeightProperty("weight")
            .concurrency(1)
            .build();

        var modelTrainer = new GraphSageModelTrainer(trainConfig, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);
        var layers = modelTrainer.train(graph, features).layers();
        var model = Model.of(
            "",
            MODEL_NAME,
            "graphSage",
            GraphSchema.empty(),
            ModelData.of(layers, new SingleLabelFeatureFunction()),
            trainConfig
        );
        ModelCatalog.set(model);

        var streamConfig = ImmutableGraphSageStreamConfig
            .builder()
            .modelName(MODEL_NAME)
            .concurrency(4)
            .batchSize(2)
            .build();

        RandomGraphGenerator randomGraphGenerator = RandomGraphGenerator.builder()
            .nodeCount(2000)
            .averageDegree(3)
            .nodePropertyProducer(PropertyProducer.randomDouble("f1", 0D, 1D))
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .relationshipPropertyProducer(PropertyProducer.fixedDouble("weight", 1.0))
            .aggregation(Aggregation.SINGLE)
            .orientation(Orientation.UNDIRECTED)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .allocationTracker(AllocationTracker.empty())
            .build();
        var trainGraph = randomGraphGenerator.generate();

        var graphSage = new GraphSage(trainGraph, streamConfig, Pools.DEFAULT, AllocationTracker.empty(), ProgressTracker.NULL_TRACKER);
        graphSage.compute();
    }

    @Test
    void testLogging() {
        var trainConfig = configBuilder
            .modelName(MODEL_NAME)
            .addFeatureProperties("f1")
            .build();

        var graphSageTrain = new GraphSageTrainAlgorithmFactory().build(
            graph,
            trainConfig,
            AllocationTracker.empty(),
            new TestLog(),
            EmptyProgressEventTracker.INSTANCE
        );

        var resultModel = graphSageTrain.compute();
        var model = Model.of(
            "",
            MODEL_NAME,
            "graphSage",
            GraphSchema.empty(),
            ModelData.of(resultModel.data().layers(), new SingleLabelFeatureFunction()),
            trainConfig
        );
        ModelCatalog.set(model);

        var streamConfig = ImmutableGraphSageStreamConfig
            .builder()
            .modelName(MODEL_NAME)
            .batchSize(1)
            .build();

        var algorithmFactory = new GraphSageAlgorithmFactory<>(TestProgressLogger.FACTORY);
        var graphSage = algorithmFactory.build(graph, streamConfig, AllocationTracker.empty(), NullLog.getInstance(), EmptyProgressEventTracker.INSTANCE);
        graphSage.compute();

        var testLogger = (TestProgressLogger) graphSage.getProgressTracker().progressLogger();
        var messagesInOrder = testLogger.getMessages(INFO);

        assertThat(messagesInOrder)
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            .containsExactly(
                "GraphSage make embeddings :: Start",
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
                "GraphSage make embeddings :: Finished"
            );
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }
}
