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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.beta.generator.PropertyProducer;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.RandomGraphGeneratorConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.model.InjectModelCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.model.ModelCatalogExtension;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageStreamConfigImpl;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfigImpl;
import org.neo4j.gds.embeddings.graphsage.algo.SingleLabelGraphSageTrain;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.LongStream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;

@GdlExtension
@ModelCatalogExtension
class GraphSageTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED, graphNamePrefix = "orphan")
    private static final String ORPHAN_GRAPH = "CREATE " +
                                               "(a:P {f1: 0.0, f2: 0.0, f3: 0.0})" +
                                               ", (b:P {f1: 1.0, f2: 0.0, f3: 0.0})" +
                                               ", (c:P {f1: 1.0, f2: 0.0, f3: 0.0})" +
                                               ", (c)-[:T]->(c)";

    @Inject
    private Graph orphanGraph;

    @Inject
    private GraphStore orphanGraphStore;

    @InjectModelCatalog
    private ModelCatalog modelCatalog;

    private static final int NODE_COUNT = 20;
    private static final int FEATURES_COUNT = 1;
    private static final int EMBEDDING_DIMENSION = 64;
    private static final String MODEL_NAME = "graphSageModel";

    private Graph graph;
    private GraphStore graphStore;
    private HugeObjectArray<double[]> features;
    private GraphSageTrainConfigImpl.Builder configBuilder;

    @BeforeEach
    void setUp() {
        HugeGraph randomGraph = RandomGraphGenerator.builder()
            .nodeCount(NODE_COUNT)
            .averageDegree(3)
            .nodeLabelProducer(nodeId -> NodeLabelTokens.of("P"))
            .addNodePropertyProducer(NodeLabel.of("P"), PropertyProducer.randomDouble("f1", 0, 1))
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .relationshipPropertyProducer(PropertyProducer.fixedDouble("weight", 1.0))
            .seed(123L)
            .aggregation(Aggregation.SINGLE)
            .direction(Direction.UNDIRECTED)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .build().generate();

        graph = randomGraph;

        long nodeCount = graph.nodeCount();

        graphStore = CSRGraphStoreUtil.createFromGraph(
            DatabaseId.random(),
            randomGraph,
            "REL",
            Optional.of("weight"),
            4
        );

        features = HugeObjectArray.newArray(double[].class, nodeCount);

        Random random = new Random();
        LongStream.range(0, nodeCount).forEach(n -> features.set(n, random.doubles(FEATURES_COUNT).toArray()));

        configBuilder = GraphSageTrainConfigImpl.builder().modelUser("").embeddingDimension(EMBEDDING_DIMENSION);
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

        var trainAlgo = new SingleLabelGraphSageTrain(
            orphanGraph,
            trainConfig,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );
        var model = trainAlgo.compute();
        modelCatalog.set(model);

        var streamConfig = GraphSageStreamConfigImpl
            .builder()
            .modelUser("")
            .modelName(MODEL_NAME)
            .concurrency(4)
            .build();

        var graphSage = new GraphSageAlgorithmFactory<>(modelCatalog).build(
            orphanGraphStore,
            streamConfig,
            ProgressTracker.NULL_TRACKER
        );
        GraphSage.GraphSageResult compute = graphSage.compute();
        for (int i = 0; i < orphanGraph.nodeCount() - 1; i++) {
            Arrays.stream(compute.embeddings().get(i)).forEach(embeddingValue -> assertThat(embeddingValue).isNotNaN());
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

        var graphSageTrain = new SingleLabelGraphSageTrain(graph, trainConfig, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);
        var model = graphSageTrain.compute();


        int predictNodeCount = 2000;
        var trainGraph = RandomGraphGenerator.builder()
            .nodeCount(predictNodeCount)
            .averageDegree(3)
            .nodePropertyProducer(PropertyProducer.randomDouble("f1", 0D, 1D))
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .relationshipPropertyProducer(PropertyProducer.fixedDouble("weight", 1.0))
            .aggregation(Aggregation.SINGLE)
            .direction(Direction.UNDIRECTED)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .build()
            .generate();

        var streamConfig = GraphSageStreamConfigImpl
            .builder()
            .modelUser("")
            .modelName(MODEL_NAME)
            .concurrency(4)
            .batchSize(2)
            .build();

        var graphSage = new GraphSage(trainGraph, model, streamConfig, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);

        assertThat(graphSage.compute().embeddings().size()).isEqualTo(predictNodeCount);
    }

    @Test
    void testLogging() {
        var trainConfig = configBuilder
            .modelName(MODEL_NAME)
            .featureProperties(List.of("f1"))
            .relationshipWeightProperty("weight")
            .build();

        var graphSageTrain = new GraphSageTrainAlgorithmFactory().build(
            graph,
            trainConfig,
            ProgressTracker.NULL_TRACKER
        );

        modelCatalog.set(graphSageTrain.compute());

        var streamConfig = GraphSageStreamConfigImpl
            .builder()
            .modelUser("")
            .modelName(MODEL_NAME)
            .batchSize(1)
            .build();

        var log = Neo4jProxy.testLog();
        var graphSage = new GraphSageAlgorithmFactory<>(modelCatalog).build(
            graphStore,
            streamConfig,
            log,
            EmptyTaskRegistryFactory.INSTANCE
        );
        graphSage.compute();

        var messagesInOrder = log.getMessages(INFO);

        assertThat(messagesInOrder)
            // avoid asserting on the thread id
            .extracting(removingThreadId())
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
}
