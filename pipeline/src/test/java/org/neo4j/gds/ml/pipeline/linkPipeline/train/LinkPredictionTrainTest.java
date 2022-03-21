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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.metrics.LinkMetric;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfigImpl;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.L2FeatureStep;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfigImpl;
import org.neo4j.gds.ml.models.randomforest.RandomForestTrainConfigImpl;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.keepingFixedNumberOfDecimals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

@GdlExtension
class LinkPredictionTrainTest {

    static String NODES = "(a:N {scalar: 0, array: [-1.0, -2.0, 1.0, 1.0, 3.0]}), " +
                          "(b:N {scalar: 4, array: [2.0, 1.0, -2.0, 2.0, 1.0]}), " +
                          "(c:N {scalar: 0, array: [-3.0, 4.0, 3.0, 3.0, 2.0]}), " +
                          "(d:N {scalar: 3, array: [1.0, 3.0, 1.0, -1.0, -1.0]}), " +
                          "(e:N {scalar: 1, array: [-2.0, 1.0, 2.0, 1.0, -1.0]}), " +
                          "(f:N {scalar: 0, array: [-1.0, -3.0, 1.0, 2.0, 2.0]}), " +
                          "(g:N {scalar: 1, array: [3.0, 1.0, -3.0, 3.0, 1.0]}), " +
                          "(h:N {scalar: 3, array: [-1.0, 3.0, 2.0, 1.0, -3.0]}), " +
                          "(i:N {scalar: 3, array: [4.0, 1.0, 1.0, 2.0, 1.0]}), " +
                          "(j:N {scalar: 4, array: [1.0, -4.0, 2.0, -2.0, 2.0]}), " +
                          "(k:N {scalar: 0, array: [2.0, 1.0, 3.0, 1.0, 1.0]}), " +
                          "(l:N {scalar: 1, array: [-1.0, 3.0, -2.0, 3.0, -2.0]}), " +
                          "(m:N {scalar: 0, array: [4.0, 4.0, 1.0, 1.0, 1.0]}), " +
                          "(n:N {scalar: 3, array: [1.0, -2.0, 3.0, 2.0, 3.0]}), " +
                          "(o:N {scalar: 2, array: [-3.0, 3.0, -1.0, -1.0, 1.0]}), ";

    @GdlGraph(graphNamePrefix = "train")
    static String GRAPH =
        "CREATE " +
        NODES +
        "(a)-[:REL {label: 1.0}]->(b), " +
        "(a)-[:REL {label: 1.0}]->(c), " +
        "(a)-[:REL {label: 0.0}]->(e), " +
        "(a)-[:REL {label: 1.0}]->(h), " +
        "(a)-[:REL {label: 1.0}]->(i), " +
        "(a)-[:REL {label: 0.0}]->(i), " +
        "(b)-[:REL {label: 0.0}]->(c), " +
        "(b)-[:REL {label: 1.0}]->(f), " +
        "(b)-[:REL {label: 1.0}]->(g), " +
        "(b)-[:REL {label: 0.0}]->(n), " +
        "(b)-[:REL {label: 1.0}]->(o), " +
        "(c)-[:REL {label: 1.0}]->(d), " +
        "(c)-[:REL {label: 1.0}]->(h), " +
        "(c)-[:REL {label: 1.0}]->(l), " +
        "(e)-[:REL {label: 0.0}]->(f), " +
        "(e)-[:REL {label: 0.0}]->(a), " +
        "(f)-[:REL {label: 0.0}]->(g), " +
        "(f)-[:REL {label: 0.0}]->(o), " +
        "(h)-[:REL {label: 0.0}]->(i), " +
        "(j)-[:REL {label: 1.0}]->(k), " +
        "(k)-[:REL {label: 0.0}]->(l), " +
        "(m)-[:REL {label: 0.0}]->(n), " +
        "(n)-[:REL {label: 0.0}]->(o) ";

    @Inject
    Graph trainGraph;

    static Stream<Arguments> paramsForEstimationsWithSplitConfigs() {
        return Stream.of(
            Arguments.of(
                "Default",
                LinkPredictionSplitConfigImpl.builder().testFraction(0.1).trainFraction(0.1).validationFolds(2).build(),
                MemoryRange.of(80_784, 2_577_824)
            ),
            Arguments.of(
                "Higher test-set",
                LinkPredictionSplitConfigImpl.builder().testFraction(0.6).trainFraction(0.1).validationFolds(2).build(),
                MemoryRange.of(183_184, 6_600_224)
            ),
            Arguments.of(
                "Higher train-set",
                LinkPredictionSplitConfigImpl.builder().testFraction(0.1).trainFraction(0.6).validationFolds(2).build(),
                MemoryRange.of(186_704, 6_133_344)
            ),
            Arguments.of(
                "Higher validation folds",
                LinkPredictionSplitConfigImpl.builder().testFraction(0.1).trainFraction(0.6).validationFolds(5).build(),
                MemoryRange.of(212_960, 6_159_600)
            )
        );
    }

    static Stream<Arguments> paramsForEstimationsWithParamSpace() {
        var llrConfigs = Stream.of(
                Map.<String, Object>of("batchSize", 10),
                Map.<String, Object>of("batchSize", 100L)
            )
            .map(LogisticRegressionTrainConfig::of)
            .collect(Collectors.toList());

        return Stream.of(
            Arguments.of("LLR batchSize 10",
                TrainingMethod.LogisticRegression, List.of(llrConfigs.get(0)), MemoryRange.of(36_256, 1_122_096)
            ),
            Arguments.of(
                "LLR batchSize 100",
                TrainingMethod.LogisticRegression,
                List.of(llrConfigs.get(1)),
                MemoryRange.of(82_336, 2_579_376)
            ),
            Arguments.of(
                "LLR batchSize 10,100",
                TrainingMethod.LogisticRegression,
                llrConfigs,
                MemoryRange.of(82_336, 2_579_376)
            ),
            Arguments.of(
                "RF",
                TrainingMethod.RandomForest,
                List.of(RandomForestTrainConfigImpl
                    .builder()
                    .maxDepth(3)
                    .minSplitSize(2)
                    .maxFeaturesRatio(1.0D)
                    .numberOfDecisionTrees(1)
                    .build()),
                MemoryRange.of(60_368, 900_128)
            )
        );
    }

    @Test
    void trainsAModel() {
        String modelName = "model";

        LinkPredictionTrainConfig trainConfig = trainingConfig(modelName);

        var result = runLinkPrediction(trainConfig);

        assertThat(result.modelSelectionStatistics().trainStats().get(LinkMetric.AUCPR).size()).isEqualTo(result
            .model()
            .customInfo()
            .trainingPipeline()
            .numberOfModelCandidates());

        var actualModel = result.model();

        assertThat(actualModel.name()).isEqualTo(modelName);
        assertThat(actualModel.algoType()).isEqualTo(LinkPredictionTrain.MODEL_TYPE);
        assertThat(actualModel.trainConfig()).isEqualTo(trainConfig);
        // length of the linkFeatures

        assertThat((LogisticRegressionData) actualModel.data())
            .extracting(llrData -> llrData.weights().data().totalSize())
            .isEqualTo(6);

        var customInfo = actualModel.customInfo();
        assertThat(result.modelSelectionStatistics().validationStats().get(LinkMetric.AUCPR))
            .satisfies(scores ->
                assertThat(scores.get(0).avg()).isNotCloseTo(scores.get(1).avg(), Percentage.withPercentage(0.2))
            );

        assertThat(customInfo.bestParameters())
            .usingRecursiveComparison()
            .isEqualTo(LogisticRegressionTrainConfig.of(Map.of("penalty", 1, "patience", 5, "tolerance", 0.00001)));
    }

    @Test
    void seededTrain() {
        String modelName = "model";

        LinkPredictionTrainConfig trainConfig = trainingConfig(modelName);

        var modelData = runLinkPrediction(trainConfig)
            .model()
            .data();
        var modelDataRepeated = runLinkPrediction(trainConfig)
            .model()
            .data();

        assertThat(modelData)
            .usingRecursiveComparison()
            .withEqualsForType(LocalIdMap::equals, LocalIdMap.class)
            .isEqualTo(modelDataRepeated);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "  10,   10, 57_424, 1_778_304",
        "  10,  100, 59_728, 1_851_168",
        "  10, 1000, 82_336, 2_579_376",
        "1000, 1000, 82_336, 2_579_376"
    })
    void estimateWithDifferentGraphSizes(int nodeCount, int relationshipCount, int expectedMinEstimation, int expectedMaxEstimation) {
        var trainConfig = LinkPredictionTrainConfig
            .builder()
            .modelName("DUMMY")
            .graphName("DUMMY")
            .pipeline("DUMMY")
            .build();

        var pipeline = new LinkPredictionPipeline();
        pipeline.addTrainerConfig(TrainingMethod.LogisticRegression, LogisticRegressionTrainConfig.defaultConfig());

        var graphDim = GraphDimensions.of(nodeCount, relationshipCount);
        MemoryTree actualEstimation = LinkPredictionTrain
            .estimate(pipeline, trainConfig)
            .estimate(graphDimensionsWithSplits(graphDim, pipeline.splitConfig()), trainConfig.concurrency());

        MemoryRange actualRange = actualEstimation.memoryUsage();
        assertThat(actualRange)
            .withFailMessage(
                "Expected (%d, %d), but got (%d, %d)",
                expectedMinEstimation,
                expectedMaxEstimation,
                actualRange.min,
                actualRange.max
            )
            .isEqualTo(MemoryRange.of(expectedMinEstimation, expectedMaxEstimation));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("paramsForEstimationsWithSplitConfigs")
    void estimateWithDifferentSplits(String desc, LinkPredictionSplitConfig splitConfig, MemoryRange expectedRange) {
        var trainConfig = LinkPredictionTrainConfig
            .builder()
            .modelName("DUMMY")
            .graphName("DUMMY")
            .pipeline("DUMMY")
            .build();

        var pipeline = new LinkPredictionPipeline();
        pipeline.addTrainerConfig(TrainingMethod.LogisticRegression, LogisticRegressionTrainConfig.defaultConfig());
        pipeline.setSplitConfig(splitConfig);

        var graphDim = GraphDimensions.of(100, 1_000);
        MemoryTree actualEstimation = LinkPredictionTrain
            .estimate(pipeline, trainConfig)
            .estimate(graphDimensionsWithSplits(graphDim, pipeline.splitConfig()), trainConfig.concurrency());

        MemoryRange actualRange = actualEstimation.memoryUsage();
        assertThat(actualRange)
            .withFailMessage(
                "Expected (%d, %d), but got (%d, %d)",
                expectedRange.min,
                expectedRange.max,
                actualRange.min,
                actualRange.max
            )
            .isEqualTo(expectedRange);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("paramsForEstimationsWithParamSpace")
    void estimateWithParameterSpace(String desc, TrainingMethod trainerMethod, List<TrainerConfig> parameterSpace, MemoryRange expectedRange) {
        var trainConfig = LinkPredictionTrainConfig
            .builder()
            .modelName("DUMMY")
            .graphName("DUMMY")
            .pipeline("DUMMY")
            .build();

        var pipeline = new LinkPredictionPipeline();
        pipeline.setTrainingParameterSpace(trainerMethod, parameterSpace);
        var graphDim = GraphDimensions.of(100, 1_000);
        MemoryTree actualEstimation = LinkPredictionTrain
            .estimate(pipeline, trainConfig)
            .estimate(graphDimensionsWithSplits(graphDim, pipeline.splitConfig()), trainConfig.concurrency());

        MemoryRange actualRange = actualEstimation.memoryUsage();
        assertThat(actualRange)
            .withFailMessage(
                "Expected (%d, %d), but got (%d, %d)",
                expectedRange.min,
                expectedRange.max,
                actualRange.min,
                actualRange.max
            )
            .isEqualTo(expectedRange);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "  1,  41_296, 1_280_016",
        "  2,  54_976, 1_713_136",
        "  4,  82_336, 2_579_376",
    })
    void estimateWithConcurrency(int concurrency, int expectedMinEstimation, int expectedMaxEstimation) {
        var trainConfig = LinkPredictionTrainConfig
            .builder()
            .modelName("DUMMY")
            .graphName("DUMMY")
            .pipeline("DUMMY")
            .concurrency(concurrency)
            .build();

        var pipeline = new LinkPredictionPipeline();
        pipeline.addTrainerConfig(TrainingMethod.LogisticRegression, LogisticRegressionTrainConfig.defaultConfig());

        var graphDim = GraphDimensions.of(100, 1_000);
        MemoryTree actualEstimation = LinkPredictionTrain
            .estimate(pipeline, trainConfig)
            .estimate(graphDimensionsWithSplits(graphDim, pipeline.splitConfig()), trainConfig.concurrency());

        MemoryRange actualRange = actualEstimation.memoryUsage();
        assertThat(actualRange)
            .withFailMessage(
                "Expected (%d, %d), but got (%d, %d)",
                expectedMinEstimation,
                expectedMaxEstimation,
                actualRange.min,
                actualRange.max
            )
            .isEqualTo(MemoryRange.of(expectedMinEstimation, expectedMaxEstimation));
    }

    @Test
    void logProgressRF() {
        var pipeline = new LinkPredictionPipeline();
        pipeline.setSplitConfig(LinkPredictionSplitConfig.builder()
            .validationFolds(2)
            .negativeSamplingRatio(1)
            .trainFraction(0.5)
            .testFraction(0.5)
            .build());

        pipeline.addTrainerConfig(
            TrainingMethod.RandomForest,
            RandomForestTrainConfigImpl.builder().numberOfDecisionTrees(5).build()
        );

        pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));

        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            LinkPredictionTrain.progressTask(),
            log,
            1,
            EmptyTaskRegistryFactory.INSTANCE
        );

        new LinkPredictionTrain(
            trainGraph,
            trainGraph,
            pipeline,
            LinkPredictionTrainConfig
                .builder()
                .modelName("DUMMY")
                .graphName("DUMMY")
                .pipeline("DUMMY")
                .concurrency(4)
                .build(),
            progressTracker
        ).compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .contains(
                "LinkPredictionTrain :: train best model :: Start",
                "LinkPredictionTrain :: train best model :: trained decision tree 1 out of 5",
                "LinkPredictionTrain :: train best model :: trained decision tree 2 out of 5",
                "LinkPredictionTrain :: train best model :: trained decision tree 3 out of 5",
                "LinkPredictionTrain :: train best model :: trained decision tree 4 out of 5",
                "LinkPredictionTrain :: train best model :: trained decision tree 5 out of 5",
                "LinkPredictionTrain :: train best model :: Finished"
            );
    }

    @Test
    void logProgressLR() {
        var pipeline = new LinkPredictionPipeline();
        pipeline.setSplitConfig(LinkPredictionSplitConfig.builder()
            .validationFolds(2)
            .negativeSamplingRatio(1)
            .trainFraction(0.5)
            .testFraction(0.5)
            .build());

        pipeline.addTrainerConfig(TrainingMethod.LogisticRegression,
            LogisticRegressionTrainConfigImpl.builder().maxEpochs(5).minEpochs(5).build()
        );

        pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));

        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            LinkPredictionTrain.progressTask(),
            log,
            1,
            EmptyTaskRegistryFactory.INSTANCE
        );

        new LinkPredictionTrain(
            trainGraph,
            trainGraph,
            pipeline,
            LinkPredictionTrainConfig
                .builder()
                .modelName("DUMMY")
                .graphName("DUMMY")
                .pipeline("DUMMY")
                .concurrency(4)
                .build(),
            progressTracker
        ).compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(keepingFixedNumberOfDecimals())
            .contains(
                "LinkPredictionTrain :: train best model :: Start",
                "LinkPredictionTrain :: train best model :: Epoch 1 with loss 0.688097317504",
                "LinkPredictionTrain :: train best model :: Epoch 2 with loss 0.683213654690",
                "LinkPredictionTrain :: train best model :: Epoch 3 with loss 0.678498422872",
                "LinkPredictionTrain :: train best model :: Epoch 4 with loss 0.673952840083",
                "LinkPredictionTrain :: train best model :: Epoch 5 with loss 0.669576960529",
                "LinkPredictionTrain :: train best model :: Finished"
            );
    }

    private LinkPredictionPipeline linkPredictionPipeline() {
        LinkPredictionPipeline pipeline = new LinkPredictionPipeline();

        pipeline.setSplitConfig(LinkPredictionSplitConfig.builder()
            .validationFolds(2)
            .negativeSamplingRatio(1)
            .trainFraction(0.5)
            .testFraction(0.5)
            .build());

        pipeline.setTrainingParameterSpace(TrainingMethod.LogisticRegression, List.of(
            LogisticRegressionTrainConfig.of(Map.of("penalty", 1, "patience", 5, "tolerance", 0.00001)),
            LogisticRegressionTrainConfig.of(Map.of("penalty", 100, "patience", 5, "tolerance", 0.00001))
        ));
        // Should NOT be the winning model, so give it bad hyperparams.
        pipeline.setTrainingParameterSpace(TrainingMethod.RandomForest, List.of(
            RandomForestTrainConfigImpl
                .builder()
                .minSplitSize(2)
                .maxDepth(1)
                .numberOfDecisionTrees(1)
                .maxFeaturesRatio(0.1)
                .build()
        ));

        pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));
        return pipeline;
    }

    private LinkPredictionTrainConfig trainingConfig(String modelName) {
        return LinkPredictionTrainConfig
            .builder()
            .modelName(modelName)
            .graphName("g")
            .pipeline("DUMMY")
            .negativeClassWeight(1)
            .randomSeed(1337L)
            .build();
    }

    private LinkPredictionTrainResult runLinkPrediction(
        LinkPredictionTrainConfig trainConfig
    ) {
        var linkPredictionTrain = new LinkPredictionTrain(
            trainGraph,
            trainGraph,
            linkPredictionPipeline(),
            trainConfig,
            ProgressTracker.NULL_TRACKER
        );

        return linkPredictionTrain.compute();
    }

    private GraphDimensions graphDimensionsWithSplits(GraphDimensions inputDimensions, LinkPredictionSplitConfig splitConfig) {
        return splitConfig.expectedGraphDimensions(inputDimensions.nodeCount(), inputDimensions.relCountUpperBound());
    }
}
