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

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.ResourceUtil;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.metrics.classification.F1Weighted;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfigImpl;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainerConfig;
import org.neo4j.gds.ml.pipeline.AutoTuningConfigImpl;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfigImpl;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.test.TestProc;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.TestSupport.assertMemoryEstimation;
import static org.neo4j.gds.assertj.Extractors.keepingFixedNumberOfDecimals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.DEBUG;
import static org.neo4j.gds.compat.TestLog.INFO;
import static org.neo4j.gds.ml.metrics.classification.OutOfBagError.OUT_OF_BAG_ERROR;
import static org.neo4j.gds.ml.pipeline.AutoTuningConfig.MAX_TRIALS;

class NodeClassificationTrainTest extends BaseProcTest {
    private static final String GRAPH_NAME = "g";
    private static final String GRAPH_NAME_WITH_RELATIONSHIPS = "gRel";

    @Neo4jGraph
    private static final String DB_QUERY1 =
        "CREATE " +
        "  (a1:N {bananas: 100.0, arrayProperty: [1.2, 1.2], a: 1.2, b: 1.2, t: 0})" +
        ", (a2:N {bananas: 100.0, arrayProperty: [2.8, 2.5], a: 2.8, b: 2.5, t: 0})" +
        ", (a3:N {bananas: 100.0, arrayProperty: [3.3, 0.5], a: 3.3, b: 0.5, t: 0})" +
        ", (a4:N {bananas: 100.0, arrayProperty: [1.0, 0.5], a: 1.0, b: 0.5, t: 0})" +
        ", (a5:N {bananas: 100.0, arrayProperty: [1.32, 0.5], a: 1.32, b: 0.5, t: 0})" +
        ", (a6:N {bananas: 100.0, arrayProperty: [1.3, 1.5], a: 1.3, b: 1.5, t: 1})" +
        ", (a7:N {bananas: 100.0, arrayProperty: [5.3, 10.5], a: 5.3, b: 10.5, t: 1})" +
        ", (a8:N {bananas: 100.0, arrayProperty: [1.3, 2.5], a: 1.3, b: 2.5, t: 1})" +
        ", (a9:N {bananas: 100.0, arrayProperty: [0.0, 66.8], a: 0.0, b: 66.8, t: 1})" +
        ", (a10:N {bananas: 100.0, arrayProperty: [0.1, 2.8], a: 0.1, b: 2.8, t: 1})" +
        ", (a11:N {bananas: 100.0, arrayProperty: [0.66, 2.8], a: 0.66, b: 2.8, t: 1})" +
        ", (a12:N {bananas: 100.0, arrayProperty: [2.0, 10.8], a: 2.0, b: 10.8, t: 1})" +
        ", (a13:N {bananas: 100.0, arrayProperty: [5.0, 7.8], a: 5.0, b: 7.8, t: 1})" +
        ", (a14:N {bananas: 100.0, arrayProperty: [4.0, 5.8], a: 4.0, b: 5.8, t: 1})" +
        ", (a15:N {bananas: 100.0, arrayProperty: [1.0, 0.9], a: 1.0, b: 0.9, t: 1})" +
        ", (b1:M {scalar: 1.2, array: [1.0, -1.0], t: 0})" +
        ", (b2:M {scalar: 0.5, array: [1.0, -1.0], t: 0})" +
        ", (b3:M {scalar: 1.1, array: [1.0, -1.0], t: 0})" +
        ", (b4:M {scalar: 0.8, array: [1.0, -1.0], t: 0})" +
        ", (b5:M {scalar: 1.3, array: [1.0, -1.0], t: 1})" +
        ", (b6:M {scalar: 1.0, array: [2.0, -1.0], t: 1})" +
        ", (b7:M {scalar: 0.8, array: [2.0, -1.0], t: 1})" +
        ", (b8:M {scalar: 1.5, array: [2.0, -1.0], t: 1})" +
        ", (b9:M {scalar: 0.5, array: [2.0, -1.0], t: 1})" +
        ", (b1)-[:R]->(b2)" +
        ", (b1)-[:R]->(b4)" +
        ", (b3)-[:R]->(b5)" +
        ", (b5)-[:R]->(b8)" +
        ", (b4)-[:R]->(b6)" +
        ", (b4)-[:R]->(b9)" +
        ", (b2)-[:R]->(b8)";


    static final NodePropertyPredictionSplitConfig SPLIT_CONFIG = NodePropertyPredictionSplitConfigImpl
        .builder()
        .testFraction(0.33)
        .validationFolds(2)
        .build();

    private Graph graph;
    private GraphStore graphStore;

    private GraphStore graphStoreWithRelationships;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class);

        runQuery(GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("N")
            .withNodeProperties(List.of("arrayProperty", "bananas", "a", "b", "t"), DefaultValue.DEFAULT)
            .yields());

        graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();
        graph = graphStore.getUnion();

        runQuery(GdsCypher.call(GRAPH_NAME_WITH_RELATIONSHIPS)
            .graphProject()
            .withNodeLabel("M")
            .withRelationshipType("R")
            .withNodeProperties(List.of("array", "scalar", "t"), DefaultValue.DEFAULT)
            .yields());

        graphStoreWithRelationships = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME_WITH_RELATIONSHIPS).graphStore();
    }

    private Model<?, ?, NodeClassificationPipelineModelInfo> getModel(
        GraphStore graphStore,
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        TestProc caller
    ) {
        var algoFactory = new NodeClassificationTrainPipelineAlgorithmFactory(caller.executionContext());
        var algo = algoFactory.build(graphStore, config, pipeline, ProgressTracker.NULL_TRACKER);
        return algo.compute().model();
    }

    @Test
    void runWithOnlyOOBError() {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.addFeatureStep(NodeFeatureStep.of("array"));

        var metricSpecification = ClassificationMetricSpecification.Parser.parse("OUT_OF_BAG_ERROR");

        var modelCandidate = RandomForestClassifierTrainerConfig.DEFAULT;
        pipeline.addTrainerConfig(modelCandidate);

        pipeline.setSplitConfig(NodePropertyPredictionSplitConfigImpl.builder()
            .testFraction(0.3)
            .validationFolds(2)
            .build()
        );

        var config = createConfig(
            "model",
            GRAPH_NAME_WITH_RELATIONSHIPS,
            metricSpecification,
            1L
        );

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var actualModel = getModel(graphStoreWithRelationships, pipeline, config, caller);
            assertThat(actualModel.customInfo().toMap()).containsEntry("metrics",
                Map.of("OUT_OF_BAG_ERROR", Map.of(
                    "test", 0.5,
                    "validation", Map.of("avg", 0.8333333333333333, "max", 1.0, "min", 0.6666666666666666))
                )
            );
            assertThat((Map) actualModel.customInfo().toMap().get("metrics")).containsOnlyKeys("OUT_OF_BAG_ERROR");
        });
    }

    @ParameterizedTest
    @MethodSource("metricArguments")
    void selectsTheBestModel(ClassificationMetricSpecification metricSpecification) {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeFeatureStep.of("a"));
        pipeline.addFeatureStep(NodeFeatureStep.of("b"));

        LogisticRegressionTrainConfig expectedWinner = LogisticRegressionTrainConfigImpl
            .builder()
            .penalty(1 * 2.0 / 3.0 * 0.5)
            .maxEpochs(10000)
            .tolerance(1e-5)
            .build();
        pipeline.addTrainerConfig(expectedWinner);

        // Should NOT be the winning model, so give bad hyperparams.
        pipeline.addTrainerConfig(
            LogisticRegressionTrainConfigImpl.builder().penalty(1 * 2.0 / 3.0 * 0.5).maxEpochs(1).build()
        );
        pipeline.addTrainerConfig(
            TunableTrainerConfig.of(
                Map.of(
                    "minSplitSize", 2000,
                    "maxDepth", 1,
                    "numberOfDecisionTrees", 1,
                    "numberOfSamplesRatio", 0.1,
                    "maxFeaturesRatio", 0.1
                ),
                TrainingMethod.RandomForestClassification
            ));
        pipeline.addTrainerConfig(
            TunableTrainerConfig.of(
                Map.of(
                    "minSplitSize", 2000,
                    "maxDepth", 1,
                    "numberOfDecisionTrees", 1,
                    "numberOfSamplesRatio", 0.1,
                    "maxFeaturesRatio", Map.of("range", List.of(0.05, 0.1))
                ),
                TrainingMethod.RandomForestClassification
            ));

        var config = createConfig("model", GRAPH_NAME, metricSpecification, 1L);

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var ncTrain = NodeClassificationTrain.create(
                graphStore,
                graphStore.getUnion(),
                pipeline,
                config,
                caller.executionContext(),
                ProgressTracker.NULL_TRACKER
            );

            var result = ncTrain.run();

            var metric = metricSpecification
                .createMetrics(result.classIdMap(), result.classCounts())
                .findFirst()
                .orElseThrow();

            var validationStats = result.trainingStatistics().getValidationStats(metric);

            assertThat(validationStats).hasSize(MAX_TRIALS);

            double model1Score = validationStats.get(0).avg();
            for (int i = 1; i < MAX_TRIALS; i++) {
                assertThat(model1Score)
                    .isNotCloseTo(validationStats.get(i).avg(), Percentage.withPercentage(0.2));
            }

            var actualWinnerParams = result.trainingStatistics().bestParameters();
            assertThat(actualWinnerParams.toMap()).isEqualTo(expectedWinner.toMap());
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldProduceCorrectTrainingStatistics(boolean includeOOB) {

        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeFeatureStep.of("a"));
        pipeline.addFeatureStep(NodeFeatureStep.of("b"));

        pipeline.addTrainerConfig(
            LogisticRegressionTrainConfigImpl.builder().penalty(1 * 2.0 / 3.0 * 0.5).maxEpochs(1).build()
        );

        LogisticRegressionTrainConfig expectedWinner = LogisticRegressionTrainConfigImpl
            .builder()
            .penalty(1 * 2.0 / 3.0 * 0.5)
            .maxEpochs(10000)
            .tolerance(1e-5)
            .build();
        pipeline.addTrainerConfig(expectedWinner);

        // Should NOT be the winning model, so give it bad hyperparams.
        pipeline.addTrainerConfig(
            TunableTrainerConfig.of(
                Map.of(
                    "minSplitSize", 2,
                    "maxDepth", 1,
                    "numberOfDecisionTrees", 1,
                    "maxFeaturesRatio", 0.1
                ),
                TrainingMethod.RandomForestClassification
            ));
        pipeline.addTrainerConfig(
            TunableTrainerConfig.of(
                Map.of(
                    "minSplitSize", 2,
                    "maxDepth", 1,
                    "numberOfDecisionTrees", 1,
                    "maxFeaturesRatio", Map.of("range", List.of(0.05, 0.1))
                ),
                TrainingMethod.RandomForestClassification
            ));

        var config = NodeClassificationPipelineTrainConfigImpl.builder()
            .pipeline("")
            .graphName(GRAPH_NAME)
            .username(getUsername())
            .modelName("anyThing")
            .concurrency(1)
            .randomSeed(42L)
            .targetProperty("t")
            .metrics(
                includeOOB
                    ? List.of(
                    ClassificationMetricSpecification.Parser.parse("F1_WEIGHTED"),
                    ClassificationMetricSpecification.Parser.parse("OUT_OF_BAG_ERROR"))
                    : List.of(ClassificationMetricSpecification.Parser.parse("F1_WEIGHTED"))
            )
            .build();

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var ncTrain = NodeClassificationTrain.create(
                graphStore,
                graphStore.getUnion(),
                pipeline,
                config,
                caller.executionContext(),
                ProgressTracker.NULL_TRACKER
            );

            var result = ncTrain.run();

            var trainingStatistics = result.trainingStatistics();

            var F1_WEIGHTED = new F1Weighted(result.classIdMap(), result.classCounts());

            assertThat(trainingStatistics.getBestTrialIdx()).isBetween(0, 10);
            assertThat(trainingStatistics.getValidationStats(F1_WEIGHTED))
                .hasSize(10)
                .noneMatch(Objects::isNull);
            assertThat(trainingStatistics.getTrainStats(F1_WEIGHTED))
                .hasSize(10)
                .noneMatch(Objects::isNull);

            assertThat(trainingStatistics.winningModelOuterTrainMetrics()).containsKeys(F1_WEIGHTED);
            assertThat(trainingStatistics.winningModelTestMetrics()).containsOnlyKeys(F1_WEIGHTED);

            if (includeOOB) {
                assertThat(trainingStatistics.getValidationStats(OUT_OF_BAG_ERROR)).hasSize(10);
                assertThat(trainingStatistics.getTrainStats(OUT_OF_BAG_ERROR)).containsOnlyNulls();
            } else {
                assertThat(trainingStatistics.getValidationStats(OUT_OF_BAG_ERROR)).containsOnlyNulls();
            }
        });
    }

    @Test
    void shouldComputeOOBForRF() {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeFeatureStep.of("a"));

        pipeline.addTrainerConfig(
            TunableTrainerConfig.of(
                Map.of(
                    "minSplitSize", 2,
                    "maxDepth", 1,
                    "numberOfDecisionTrees", 1,
                    "maxFeaturesRatio",  0.1
                ),
                TrainingMethod.RandomForestClassification
            ));

        var config = NodeClassificationPipelineTrainConfigImpl.builder()
            .pipeline("")
            .graphName(GRAPH_NAME)
            .username(getUsername())
            .modelName("anyThing")
            .concurrency(1)
            .randomSeed(42L)
            .targetProperty("t")
            .metrics(List.of(ClassificationMetricSpecification.Parser.parse("OUT_OF_BAG_ERROR")))
            .build();

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var ncTrain = NodeClassificationTrain.create(
                graphStore,
                graphStore.getUnion(),
                pipeline,
                config,
                caller.executionContext(),
                ProgressTracker.NULL_TRACKER
            );

            var result = ncTrain.run();

            var trainingStatistics = result.trainingStatistics();

            assertThat(trainingStatistics.getValidationStats(OUT_OF_BAG_ERROR)).hasSize(1).noneMatch(Objects::isNull);
            assertThat(trainingStatistics.getTrainStats(OUT_OF_BAG_ERROR)).hasSize(1).containsOnlyNulls();

            assertThat(trainingStatistics.winningModelOuterTrainMetrics()).isEmpty();
            assertThat(trainingStatistics.winningModelTestMetrics().keySet()).containsExactly(OUT_OF_BAG_ERROR);
        });
    }

    @ParameterizedTest
    @MethodSource("metricArguments")
    void shouldProduceDifferentMetricsForDifferentTrainings(ClassificationMetricSpecification metricSpecification) {
        var bananasPipeline = new NodeClassificationTrainingPipeline();
        bananasPipeline.setSplitConfig(SPLIT_CONFIG);

        bananasPipeline.addFeatureStep(NodeFeatureStep.of("bananas"));

        var modelCandidates = List.of(
            Map.<String, Object>of("penalty", 0.0625, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 0.125, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 0.25, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 0.5, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 1.0, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 2.0, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 4.0, "maxEpochs", 1000)
        );

        modelCandidates
            .stream()
            .map(LogisticRegressionTrainConfig::of)
            .forEach(bananasPipeline::addTrainerConfig);

        var bananasConfig = createConfig("bananasModel", GRAPH_NAME, metricSpecification, 1337L);

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var bananasTrain = NodeClassificationTrain.create(
                graphStore,
                graphStore.getUnion(),
                bananasPipeline,
                bananasConfig,
                caller.executionContext(),
                ProgressTracker.NULL_TRACKER
            );

            var arrayPipeline = new NodeClassificationTrainingPipeline();
            arrayPipeline.setSplitConfig(SPLIT_CONFIG);

            arrayPipeline.addFeatureStep(NodeFeatureStep.of("arrayProperty"));

            modelCandidates
                .stream()
                .map(LogisticRegressionTrainConfig::of)
                .forEach(arrayPipeline::addTrainerConfig);


            var arrayPropertyConfig = createConfig(
                "arrayPropertyModel",
                GRAPH_NAME,
                metricSpecification,
                42L
            );
            var arrayPropertyTrain = NodeClassificationTrain.create(
                graphStore,
                graphStore.getUnion(),
                arrayPipeline,
                arrayPropertyConfig,
                caller.executionContext(),
                ProgressTracker.NULL_TRACKER
            );

            var bananasModelTrainResult = bananasTrain.run();
            var bananasClassifier = bananasModelTrainResult.classifier();
            var arrayModelTrainResult = arrayPropertyTrain.run();
            var arrayPropertyClassifier = arrayModelTrainResult.classifier();

            assertThat(arrayPropertyClassifier)
                .usingRecursiveComparison()
                .withFailMessage("The trained classifiers are exactly the same instance!")
                .isNotSameAs(bananasClassifier);

            assertThat(arrayPropertyClassifier.data())
                .usingRecursiveComparison()
                .withFailMessage("Should not produce the same trained `data`!")
                .isNotEqualTo(bananasClassifier.data());

            var metric = metricSpecification
                .createMetrics(bananasModelTrainResult.classIdMap(), bananasModelTrainResult.classCounts())
                .findFirst()
                .orElseThrow();
            var bananasMetrics = bananasModelTrainResult
                .trainingStatistics()
                .bestCandidate()
                .trainingStats()
                .get(metric);
            var arrayPropertyMetrics = arrayModelTrainResult
                .trainingStatistics()
                .bestCandidate()
                .trainingStats()
                .get(metric);

            assertThat(arrayPropertyMetrics)
                .usingRecursiveComparison()
                .isNotSameAs(bananasMetrics)
                .isNotEqualTo(bananasMetrics);
        });
    }

    @Test
    void shouldLogProgress() {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeFeatureStep.of("bananas"));

        pipeline.addTrainerConfig(
            LogisticRegressionTrainConfig.of(Map.of("penalty", 0.0625 * 2.0 / 3.0 * 0.5, "maxEpochs", 100))
        );
        pipeline.addTrainerConfig(
            LogisticRegressionTrainConfig.of(Map.of("penalty", 0.125 * 2.0 / 3.0 * 0.5, "maxEpochs", 100))
        );

        var metrics = ClassificationMetricSpecification.Parser.parse("F1(class=1)");
        var config = createConfig("bananasModel", GRAPH_NAME, metrics, 42L);

        var progressTask = progressTask(
            pipeline,
            graph.nodeCount()
        );
        var testLog = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(progressTask, testLog, 1, EmptyTaskRegistryFactory.INSTANCE);
        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            NodeClassificationTrain.create(
                graphStore,
                graphStore.getUnion(),
                pipeline,
                config,
                caller.executionContext(),
                progressTracker
            )
                .run();

            assertThat(testLog.getMessages(INFO))
                .extracting(removingThreadId())
                .extracting(keepingFixedNumberOfDecimals(4))
                .containsExactlyElementsOf(ResourceUtil.lines("expectedLogs/node-classification-log"));
        });
    }

    @Test
    void shouldLogWarnings() {
        var pipeline = new NodeClassificationTrainingPipeline();

        pipeline.addFeatureStep(NodeFeatureStep.of("array"));
        pipeline.addFeatureStep(NodeFeatureStep.of("scalar"));
        pipeline.addTrainerConfig(LogisticRegressionTrainConfig.DEFAULT);

        var metricSpecification = ClassificationMetricSpecification.Parser.parse("F1(class=1)");

        pipeline.setSplitConfig(NodePropertyPredictionSplitConfigImpl.builder()
            .testFraction(0.3)
            .validationFolds(2)
            .build()
        );

        var config = createConfig(
            "model",
            GRAPH_NAME_WITH_RELATIONSHIPS,
            metricSpecification,
            1L
        );

        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            NodeClassificationTrainPipelineAlgorithmFactory.progressTask(graphStoreWithRelationships, pipeline),
            log,
            1,
            EmptyTaskRegistryFactory.INSTANCE
        );

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            NodeClassificationTrain.create(
                graphStoreWithRelationships,
                graphStoreWithRelationships.getUnion(),
                pipeline,
                config,
                caller.executionContext(),
                progressTracker
            ).run();

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
    void shouldLogProgressWithRange() {
        int MAX_TRIALS = 2;
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeFeatureStep.of("bananas"));

        pipeline.addTrainerConfig(
            TunableTrainerConfig.of(
                Map.of("penalty", Map.of("range", List.of(1e-4, 1e4)), "maxEpochs", 100),
                TrainingMethod.LogisticRegression
            )
        );
        pipeline.setAutoTuningConfig(AutoTuningConfigImpl.builder().maxTrials(MAX_TRIALS).build());

        var metrics = ClassificationMetricSpecification.Parser.parse("F1(class=1)");
        var config = createConfig("bananasModel", GRAPH_NAME, metrics, 42L);

        var progressTask = progressTask(pipeline, graph.nodeCount());
        var testLog = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(progressTask, testLog, 1, EmptyTaskRegistryFactory.INSTANCE);

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            NodeClassificationTrain.create(
                graphStore,
                graphStore.getUnion(),
                pipeline,
                config,
                caller.executionContext(),
                progressTracker
            )
                .run();

            assertThat(testLog.getMessages(INFO))
                .extracting(removingThreadId())
                .extracting(keepingFixedNumberOfDecimals(4))
                .containsExactlyElementsOf(ResourceUtil.lines("expectedLogs/node-classification-with-range-log-info"));

            assertThat(testLog.getMessages(DEBUG))
                .extracting(removingThreadId())
                .extracting(keepingFixedNumberOfDecimals(4))
                .containsExactlyElementsOf(ResourceUtil.lines("expectedLogs/node-classification-with-range-log-debug"));
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void seededNodeClassification(int concurrency) {
        var pipeline = new NodeClassificationTrainingPipeline();

        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeFeatureStep.of("bananas"));
        pipeline.addTrainerConfig(
            LogisticRegressionTrainConfig.of(Map.of("penalty", 0.0625, "maxEpochs", 100, "batchSize", 1))
        );

        var config = NodeClassificationPipelineTrainConfigImpl.builder()
            .pipeline("")
            .graphName(GRAPH_NAME)
            .username(getUsername())
            .modelName("model")
            .randomSeed(42L)
            .targetProperty("t")
            .metrics(List.of(ClassificationMetricSpecification.Parser.parse("Accuracy")))
            .concurrency(concurrency)
            .build();

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            Supplier<NodeClassificationTrain> algoSupplier = () -> NodeClassificationTrain.create(
                graphStore,
                        graphStore.getUnion(),
                pipeline,
                config,
                        caller.executionContext(),
                ProgressTracker.NULL_TRACKER
                    );

            var firstResult = algoSupplier.get().run();
            var secondResult = algoSupplier.get().run();

            assertThat(((LogisticRegressionData) firstResult.classifier().data()).weights().data())
                .matches(matrix -> matrix.equals(
                    ((LogisticRegressionData) secondResult.classifier().data()).weights().data(),
                    1e-10
                ));
        });
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
            .pipeline("")
            .username("myUser")
            .graphName(GRAPH_NAME_WITH_RELATIONSHIPS)
            .modelName("myModel")
            .concurrency(1)
            .randomSeed(42L)
            .targetProperty("t")
            .relationshipTypes(List.of("SOME_REL"))
            .nodeLabels(List.of("SOME_LABEL"))
            .metrics(List.of(ClassificationMetricSpecification.Parser.parse("F1_WEIGHTED")))
            .build();

        var memoryEstimation = NodeClassificationTrain.estimate(pipeline, config, new OpenModelCatalog());
        assertMemoryEstimation(
            () -> memoryEstimation,
            graphStoreWithRelationships.nodeCount(),
            graphStoreWithRelationships.relationshipCount(),
            config.concurrency(),
            memoryRange
        );
    }

    @Test
    void failEstimateOnEmptyParameterSpace() {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.featureProperties().addAll(List.of("array", "scalar"));

        var config = NodeClassificationPipelineTrainConfigImpl.builder()
            .pipeline("")
            .username("myUser")
            .graphName(GRAPH_NAME_WITH_RELATIONSHIPS)
            .modelName("myModel")
            .concurrency(1)
            .randomSeed(42L)
            .targetProperty("t")
            .relationshipTypes(List.of("SOME_REL"))
            .nodeLabels(List.of("SOME_LABEL"))
            .metrics(List.of(ClassificationMetricSpecification.Parser.parse("F1_WEIGHTED")))
            .build();

        assertThatThrownBy(() -> NodeClassificationTrain.estimate(pipeline, config, new OpenModelCatalog()))
            .hasMessage("Need at least one model candidate for training.");
    }

    public static Stream<Arguments> trainerMethodConfigs() {
        return Stream.of(
            Arguments.of(
                List.of(LogisticRegressionTrainConfig.DEFAULT.toTunableConfig()),
                MemoryRange.of(778_968, 810_928)
            ),
            Arguments.of(
                List.of(RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()),
                MemoryRange.of(90_906, 207_678)
            ),
            Arguments.of(
                List.of(LogisticRegressionTrainConfig.DEFAULT.toTunableConfig(), RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()),
                MemoryRange.of(859_936, 927_176)
            ),
            Arguments.of(
                List.of(
                    TunableTrainerConfig.of(
                        Map.of("penalty", Map.of("range", List.of(1e-4, 1e4))),
                        TrainingMethod.LogisticRegression
                    ),
                    RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()
                ),
                MemoryRange.of(859_936, 927_176)
            ),
            Arguments.of(
                List.of(
                    TunableTrainerConfig.of(
                        Map.of("batchSize", Map.of("range", List.of(1, 100_000))),
                        TrainingMethod.LogisticRegression
                    ),
                    RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()
                ),
                MemoryRange.of(430_030_336, 430_097_576)
            )
        );
    }


    private static Task progressTask(NodeClassificationTrainingPipeline pipeline, long nodeCount) {
        return Tasks.task(
            "MY DUMMY TASK",
            NodeClassificationTrain.progressTasks(pipeline, nodeCount)
        );
    }

    private NodeClassificationPipelineTrainConfig createConfig(
        String modelName,
        String graphName,
        ClassificationMetricSpecification metricSpecification,
        long randomSeed
    ) {
        return NodeClassificationPipelineTrainConfigImpl.builder()
            .pipeline("")
            .graphName(graphName)
            .username(getUsername())
            .modelName(modelName)
            .concurrency(1)
            .randomSeed(randomSeed)
            .targetProperty("t")
            .metrics(List.of(metricSpecification))
            .build();
    }

    static Stream<Arguments> metricArguments() {
        var singleClassMetrics = Stream.of(Arguments.arguments(ClassificationMetricSpecification.Parser.parse("F1(class=1)")));

        var allClassMetrics = StreamSupport.stream(ClassificationMetricSpecification.Parser.allClassMetrics().spliterator(), false)
            .map(ClassificationMetricSpecification.Parser::parse)
            .map(Arguments::of);
        return Stream.concat(singleClassMetrics, allClassMetrics);
    }
}
