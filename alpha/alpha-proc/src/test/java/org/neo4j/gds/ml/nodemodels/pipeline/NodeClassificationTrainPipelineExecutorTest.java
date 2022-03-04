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
package org.neo4j.gds.ml.nodemodels.pipeline;

import org.assertj.core.util.DoubleComparator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainConfig;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainPipelineAlgorithmFactory;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.nodePipeline.ImmutableNodeClassificationPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.ImmutableNodeClassificationSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.test.TestProc;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.TestSupport.assertMemoryEstimation;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

class NodeClassificationTrainPipelineExecutorTest extends BaseProcTest {
    private static String PIPELINE_NAME = "pipe";
    private static final String GRAPH_NAME = "g";

    @Neo4jGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (a1:N {scalar: 1.2, array: [1.0, -1.0], t: 0})" +
        ", (a2:N {scalar: 0.5, array: [1.0, -1.0], t: 0})" +
        ", (a3:N {scalar: 1.1, array: [1.0, -1.0], t: 0})" +
        ", (a4:N {scalar: 0.8, array: [1.0, -1.0], t: 0})" +
        ", (a5:N {scalar: 1.3, array: [1.0, -1.0], t: 1})" +
        ", (a6:N {scalar: 1.0, array: [2.0, -1.0], t: 1})" +
        ", (a7:N {scalar: 0.8, array: [2.0, -1.0], t: 1})" +
        ", (a8:N {scalar: 1.5, array: [2.0, -1.0], t: 1})" +
        ", (a9:N {scalar: 0.5, array: [2.0, -1.0], t: 1})" +
        ", (a1)-[:R]->(a2)" +
        ", (a1)-[:R]->(a4)" +
        ", (a3)-[:R]->(a5)" +
        ", (a5)-[:R]->(a8)" +
        ", (a4)-[:R]->(a6)" +
        ", (a4)-[:R]->(a9)" +
        ", (a2)-[:R]->(a8)";

    private GraphStore graphStore;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class);

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("N")
            .withRelationshipType("R")
            .withNodeProperties(List.of("array", "scalar", "t"), DefaultValue.DEFAULT)
            .yields();

        runQuery(createQuery);

        graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void trainsAModel() {
        var pipeline = insertPipelineIntoCatalog();
        pipeline.nodePropertySteps().add(NodePropertyStepFactory.createNodePropertyStep(
            "pageRank",
            Map.of("mutateProperty", "pr")
        ));
        pipeline.addFeatureStep(NodeClassificationFeatureStep.of("array"));
        pipeline.addFeatureStep(NodeClassificationFeatureStep.of("scalar"));
        pipeline.addFeatureStep(NodeClassificationFeatureStep.of("pr"));

        var metricSpecification = MetricSpecification.parse("F1(class=1)");
        var metric = metricSpecification.createMetrics(List.of()).findFirst().orElseThrow();

        pipeline.setTrainingParameterSpace(List.of(LogisticRegressionTrainConfig.of(
            Map.of("penalty", 1, "maxEpochs", 1)
        )));

        pipeline.setSplitConfig(ImmutableNodeClassificationSplitConfig.builder()
            .testFraction(0.3)
            .validationFolds(2)
            .build()
        );

        var config = createConfig(
            "model",
            metricSpecification,
            1L
        );

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var ncPipeTrain = new NodeClassificationTrainPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            );

            var result = ncPipeTrain.compute();
            var model = result.model();

            assertThat(model.creator()).isEqualTo(getUsername());

            // using explicit type intentionally :)
            NodeClassificationPipelineModelInfo customInfo = model.customInfo();
            assertThat(customInfo.metrics().get(metric).validation().toMap())
                .usingComparatorForType(new DoubleComparator(1e-10), Double.class)
                .isEqualTo(Map.of("avg",0.24999999687500002, "max",0.49999999375000004, "min",0.0));

            assertThat(customInfo.metrics().get(metric).train().toMap())
                .usingComparatorForType(new DoubleComparator(1e-10), Double.class)
                .isEqualTo(Map.of("avg",0.399999996, "max",0.799999992, "min",0.0));

            assertThat(customInfo.trainingPipeline()).isNotSameAs(pipeline);
            assertThat(customInfo.trainingPipeline())
                .usingRecursiveComparison()
                .ignoringFields("creationTime")
                .isEqualTo(pipeline);

            assertThat(customInfo.trainingPipeline().toMap()).isEqualTo(pipeline.toMap());
        });
    }

    @Test
    void passesAllParameters() {
        var config = ImmutableNodeClassificationPipelineTrainConfig.builder()
            .pipeline(PIPELINE_NAME)
            .username("myUser")
            .graphName(GRAPH_NAME)
            .modelName("myModel")
            .concurrency(1)
            .randomSeed(42L)
            .targetProperty("t")
            .addRelationshipType("SOME_REL")
            .addNodeLabel("SOME_LABEL")
            .minBatchSize(1)
            .metrics(List.of(MetricSpecification.parse("F1_WEIGHTED")))
            .build();

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var pipeline = new NodeClassificationPipeline();
            NodeClassificationTrainPipelineExecutor executor = new NodeClassificationTrainPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                "g",
                ProgressTracker.NULL_TRACKER
            );

            NodeClassificationTrainConfig actualConfig = NodeClassificationTrainPipelineExecutor.innerConfig(pipeline, config);

            assertThat(actualConfig)
                .matches(innerConfig -> innerConfig.username().equals(config.username()))
                .matches(innerConfig -> innerConfig.modelName().equals("myModel"))
                .matches(innerConfig -> innerConfig.concurrency() == 1 )
                .matches(innerConfig -> innerConfig.randomSeed().orElseThrow().equals(42L) )
                .matches(innerConfig -> innerConfig.targetProperty().equals("t") )
                .matches(
                    innerConfig -> innerConfig.relationshipTypes().equals(List.of("SOME_REL")),
                    actualConfig.relationshipTypes().toString()
                )
                .matches(innerConfig -> innerConfig.nodeLabels().equals(List.of("SOME_LABEL")) )
                .matches(innerConfig -> {
                    List<String> metricNames = innerConfig.metrics().stream()
                        .map(MetricSpecification::asString)
                        .collect(Collectors.toList());
                    return metricNames.equals(List.of("F1_WEIGHTED"));
                });
        });
    }

    @Test
    void shouldLogProgress() {
        var pipeline = insertPipelineIntoCatalog();

        pipeline.addFeatureStep(NodeClassificationFeatureStep.of("array"));
        pipeline.addFeatureStep(NodeClassificationFeatureStep.of("scalar"));

        var metricSpecification = MetricSpecification.parse("F1(class=1)");

        pipeline.setSplitConfig(ImmutableNodeClassificationSplitConfig.builder()
            .testFraction(0.3)
            .validationFolds(2)
            .build()
        );

        var config = createConfig(
            "model",
            metricSpecification,
            1L
        );

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var log = Neo4jProxy.testLog();
            var progressTracker = new TestProgressTracker(
                new NodeClassificationTrainPipelineAlgorithmFactory(caller.executionContext()).progressTask(
                    graphStore,
                    config
                ),
                log,
                1,
                EmptyTaskRegistryFactory.INSTANCE
            );

            NodeClassificationTrainPipelineExecutor executor = new NodeClassificationTrainPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                "g",
                progressTracker
            );

            executor.compute();

            assertThat(log.getMessages(TestLog.WARN))
                .extracting(removingThreadId())
                .containsExactly(
                    "Node Classification Train Pipeline :: NCTrain :: ShuffleAndSplit :: The specified `testFraction` leads to a very small test set with only 3 node(s). " +
                    "Proceeding with such a small set might lead to unreliable results.",
                    "Node Classification Train Pipeline :: NCTrain :: ShuffleAndSplit :: The specified `validationFolds` leads to very small validation sets with only 3 node(s). " +
                    "Proceeding with such small sets might lead to unreliable results."
                );

            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .contains(
                    "Node Classification Train Pipeline :: NCTrain :: ShuffleAndSplit :: Train set size is 6",
                    "Node Classification Train Pipeline :: NCTrain :: ShuffleAndSplit :: Test set size is 3"
                );
        });
    }

    @Test
    void failsOnInvalidTargetProperty() {
        var pipeline = insertPipelineIntoCatalog();
        pipeline.featureProperties().addAll(List.of("array"));

        var config = ImmutableNodeClassificationPipelineTrainConfig.builder()
            .pipeline(PIPELINE_NAME)
            .graphName(GRAPH_NAME)
            .modelName("myModel")
            .targetProperty("INVALID_PROPERTY")
            .addMetric(MetricSpecification.parse("F1(class=1)"))
            .build();

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var ncPipeTrain = new NodeClassificationTrainPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            );
            assertThatThrownBy(ncPipeTrain::compute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Target property `INVALID_PROPERTY` not found in graph with node properties:");
        });
    }

    @Test
    void shouldEstimateMemory() {
        var pipeline = insertPipelineIntoCatalog();
        pipeline.nodePropertySteps().add(NodePropertyStepFactory.createNodePropertyStep(
            "pageRank",
            Map.of("mutateProperty", "pr")
        ));
        pipeline.nodePropertySteps().add(NodePropertyStepFactory.createNodePropertyStep(
            "wcc",
            Map.of("mutateProperty", "myNewProp", "threshold", 0.42F, "relationshipWeightProperty", "weight")
        ));
        pipeline.featureProperties().addAll(List.of("array", "scalar", "pr"));

        var config = ImmutableNodeClassificationPipelineTrainConfig.builder()
            .pipeline(PIPELINE_NAME)
            .username("myUser")
            .graphName(GRAPH_NAME)
            .modelName("myModel")
            .concurrency(1)
            .randomSeed(42L)
            .targetProperty("t")
            .addRelationshipType("SOME_REL")
            .addNodeLabel("SOME_LABEL")
            .minBatchSize(1)
            .metrics(List.of(MetricSpecification.parse("F1_WEIGHTED")))
            .build();

        var memoryEstimation = NodeClassificationTrainPipelineExecutor.estimate(pipeline, config, new OpenModelCatalog());
        assertMemoryEstimation(
            () -> memoryEstimation,
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            config.concurrency(),
            31742464L,
            31774424L
        );

    }
    private NodeClassificationPipeline insertPipelineIntoCatalog() {
        var info = new NodeClassificationPipeline();
        PipelineCatalog.set(getUsername(), PIPELINE_NAME, info);
        return info;
    }

    private NodeClassificationPipelineTrainConfig createConfig(
        String modelName,
        MetricSpecification metricSpecification,
        long randomSeed
    ) {
        return ImmutableNodeClassificationPipelineTrainConfig.builder()
            .pipeline(PIPELINE_NAME)
            .graphName(GRAPH_NAME)
            .modelName(modelName)
            .concurrency(1)
            .randomSeed(randomSeed)
            .targetProperty("t")
            .metrics(List.of(metricSpecification))
            .build();
    }
}
