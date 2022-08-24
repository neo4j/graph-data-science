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
package org.neo4j.gds.ml.pipeline.nodePipeline.classification.train;

import org.assertj.core.data.Offset;
import org.assertj.core.util.DoubleComparator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainerConfig;
import org.neo4j.gds.ml.pipeline.AutoTuningConfigImpl;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfigImpl;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.test.TestProc;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.TestSupport.assertMemoryEstimation;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

class NodeClassificationTrainPipelineExecutorTest extends BaseProcTest {
    private static final String PIPELINE_NAME = "pipe";
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

    @Test
    void trainsAModel() {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.nodePropertySteps().add(NodePropertyStepFactory.createNodePropertyStep(
            "testProc",
            Map.of("mutateProperty", "pr")
        ));
        pipeline.addFeatureStep(NodeFeatureStep.of("array"));
        pipeline.addFeatureStep(NodeFeatureStep.of("scalar"));
        pipeline.addFeatureStep(NodeFeatureStep.of("pr"));

        var metricSpecification = ClassificationMetricSpecification.parse("F1(class=1)");
        var metric = metricSpecification.createMetrics(List.of()).findFirst().orElseThrow();

        var modelCandidate = LogisticRegressionTrainConfig.of(Map.of("penalty", 1, "maxEpochs", 1));
        pipeline.addTrainerConfig(modelCandidate);

        pipeline.setSplitConfig(NodePropertyPredictionSplitConfigImpl.builder()
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
            assertThat(model.algoType()).isEqualTo(NodeClassificationTrainingPipeline.MODEL_TYPE);
            assertThat(model.data()).isInstanceOf(LogisticRegressionData.class);
            assertThat(model.trainConfig()).isEqualTo(config);
            assertThat(model.graphSchema()).isEqualTo(graphStore.schema());
            assertThat(model.name()).isEqualTo("model");
            assertThat(model.stored()).isFalse();
            assertThat(model.customInfo().bestParameters().toMap()).isEqualTo(modelCandidate.toMap());
            assertThat(model.customInfo().metrics().keySet()).containsExactly(metric.toString());
            assertThat(((Map) model.customInfo().metrics().get(metric.toString())).keySet())
                .containsExactlyInAnyOrder("train", "validation", "outerTrain", "test");

            // using explicit type intentionally :)
            NodeClassificationPipelineModelInfo customInfo = model.customInfo();
            var testScore = (double) ((Map) customInfo.metrics().get(metric.toString())).get("test");
            assertThat(testScore).isCloseTo(0.499999, Offset.offset(1e-5));
            var outerTrainScore = (double) ((Map) customInfo.metrics().get(metric.toString())).get("outerTrain");
            assertThat(outerTrainScore).isCloseTo(0.799999, Offset.offset(1e-5));
            var validationStats = (Map) ((Map) customInfo.metrics().get(metric.toString())).get("validation");
            var trainStats = (Map) ((Map) customInfo.metrics().get(metric.toString())).get("train");
            assertThat(validationStats)
                .usingRecursiveComparison()
                .withComparatorForType(new DoubleComparator(1e-5), Double.class)
                .isEqualTo(Map.of("avg",0.799999992, "max",0.799999992, "min",0.799999992));

            assertThat(trainStats)
                .usingRecursiveComparison()
                .withComparatorForType(new DoubleComparator(1e-5), Double.class)
                .isEqualTo(Map.of("avg",0.799999992, "max",0.799999992, "min",0.799999992));

            assertThat(customInfo.pipeline().nodePropertySteps()).isEqualTo(pipeline.nodePropertySteps());
            assertThat(customInfo.pipeline().featureProperties()).isEqualTo(pipeline.featureProperties());
        });
    }

    @Test
    void runWithOnlyOOBError() {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.addFeatureStep(NodeFeatureStep.of("array"));

        var metricSpecification = ClassificationMetricSpecification.parse("OUT_OF_BAG_ERROR");

        var modelCandidate = RandomForestClassifierTrainerConfig.DEFAULT;
        pipeline.addTrainerConfig(modelCandidate);

        pipeline.setSplitConfig(NodePropertyPredictionSplitConfigImpl.builder()
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

            var actualModel = ncPipeTrain.compute().model();
            assertThat(actualModel.customInfo().toMap()).containsEntry("metrics",
                Map.of("OUT_OF_BAG_ERROR", Map.of(
                        "test", 0.3333333333333333,
                        "validation", Map.of("avg", 0.3333333333333333, "max", 0.3333333333333333, "min", 0.3333333333333333))
                )
            );
            assertThat((Map) actualModel.customInfo().toMap().get("metrics")).containsOnlyKeys("OUT_OF_BAG_ERROR");
        });
    }

    @Test
    void shouldLogProgress() {
        var pipeline = new NodeClassificationTrainingPipeline();

        pipeline.addFeatureStep(NodeFeatureStep.of("array"));
        pipeline.addFeatureStep(NodeFeatureStep.of("scalar"));
        pipeline.addTrainerConfig(LogisticRegressionTrainConfig.DEFAULT);

        var metricSpecification = ClassificationMetricSpecification.parse("F1(class=1)");

        pipeline.setSplitConfig(NodePropertyPredictionSplitConfigImpl.builder()
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
                NodeClassificationTrainPipelineExecutor.progressTask("Node Classification Train Pipeline", pipeline, graphStore.nodeCount()),
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
                    "Node Classification Train Pipeline :: The specified `testFraction` leads to a very small test set with only 3 node(s). " +
                    "Proceeding with such a small set might lead to unreliable results.",
                    "Node Classification Train Pipeline :: The specified `validationFolds` leads to very small validation sets with only 3 node(s). " +
                    "Proceeding with such small sets might lead to unreliable results."
                );

            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .contains(
                    "Node Classification Train Pipeline :: Train set size is 6",
                    "Node Classification Train Pipeline :: Test set size is 3"
                );
        });
    }

    @Test
    void failsOnInvalidTargetProperty() {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.featureProperties().add("array");

        var config = NodeClassificationPipelineTrainConfigImpl.builder()
            .username(getUsername())
            .pipeline(PIPELINE_NAME)
            .graphName(GRAPH_NAME)
            .modelName("myModel")
            .targetProperty("INVALID_PROPERTY")
            .metrics(List.of(ClassificationMetricSpecification.parse("F1(class=1)")))
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


    public static Stream<Arguments> trainerMethodConfigs() {
        return Stream.of(
            Arguments.of(
                List.of(LogisticRegressionTrainConfig.DEFAULT.toTunableConfig()),
                MemoryRange.of(779_232, 811_192)
            ),
            Arguments.of(
                List.of(RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()),
                MemoryRange.of(90_938, 207_710)
            ),
            Arguments.of(
                List.of(LogisticRegressionTrainConfig.DEFAULT.toTunableConfig(), RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()),
                MemoryRange.of(860_200, 927_440)
            ),
            Arguments.of(
                List.of(
                    TunableTrainerConfig.of(
                        Map.of("penalty", Map.of("range", List.of(1e-4, 1e4))),
                        TrainingMethod.LogisticRegression
                    ),
                    RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()
                ),
                MemoryRange.of(940_200, 1_007_440)
            ),
            Arguments.of(
                List.of(
                    TunableTrainerConfig.of(
                        Map.of("batchSize", Map.of("range", List.of(1, 100_000))),
                        TrainingMethod.LogisticRegression
                    ),
                    RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()
                ),
                MemoryRange.of(430_110_600, 430_177_840)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("trainerMethodConfigs")
    void shouldEstimateMemory(List<TunableTrainerConfig> tunableConfigs, MemoryRange memoryRange) {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.nodePropertySteps().add(NodePropertyStepFactory.createNodePropertyStep(
            "testProc",
            Map.of("mutateProperty", "pr")
        ));
        pipeline.nodePropertySteps().add(NodePropertyStepFactory.createNodePropertyStep(
            "testProc", Map.of("mutateProperty", "myNewProp"))
        );
        pipeline.featureProperties().addAll(List.of("array", "scalar", "pr"));

        for (TunableTrainerConfig tunableConfig : tunableConfigs) {
            pipeline.addTrainerConfig(tunableConfig);
        }

        // Limit maxTrials to make comparison with concrete-only parameter spaces easier.
        pipeline.setAutoTuningConfig(AutoTuningConfigImpl.builder().maxTrials(2).build());

        var config = NodeClassificationPipelineTrainConfigImpl.builder()
            .pipeline(PIPELINE_NAME)
            .username("myUser")
            .graphName(GRAPH_NAME)
            .modelName("myModel")
            .concurrency(1)
            .randomSeed(42L)
            .targetProperty("t")
            .relationshipTypes(List.of("SOME_REL"))
            .nodeLabels(List.of("SOME_LABEL"))
            .metrics(List.of(ClassificationMetricSpecification.parse("F1_WEIGHTED")))
            .build();

        var memoryEstimation = NodeClassificationTrainPipelineExecutor.estimate(pipeline, config, new OpenModelCatalog());
        assertMemoryEstimation(
            () -> memoryEstimation,
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            config.concurrency(),
            memoryRange
        );
    }

    @Test
    void failEstimateOnEmptyParameterSpace() {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.featureProperties().addAll(List.of("array", "scalar"));

        var config = NodeClassificationPipelineTrainConfigImpl.builder()
            .pipeline(PIPELINE_NAME)
            .username("myUser")
            .graphName(GRAPH_NAME)
            .modelName("myModel")
            .concurrency(1)
            .randomSeed(42L)
            .targetProperty("t")
            .relationshipTypes(List.of("SOME_REL"))
            .nodeLabels(List.of("SOME_LABEL"))
            .metrics(List.of(ClassificationMetricSpecification.parse("F1_WEIGHTED")))
            .build();

        assertThatThrownBy(() -> NodeClassificationTrainPipelineExecutor.estimate(pipeline, config, new OpenModelCatalog()))
            .hasMessage("Need at least one model candidate for training.");
    }

    private NodeClassificationPipelineTrainConfig createConfig(
        String modelName,
        ClassificationMetricSpecification metricSpecification,
        long randomSeed
    ) {
        return NodeClassificationPipelineTrainConfigImpl.builder()
            .pipeline(PIPELINE_NAME)
            .graphName(GRAPH_NAME)
            .username(getUsername())
            .modelName(modelName)
            .concurrency(1)
            .randomSeed(randomSeed)
            .targetProperty("t")
            .metrics(List.of(metricSpecification))
            .build();
    }
}
