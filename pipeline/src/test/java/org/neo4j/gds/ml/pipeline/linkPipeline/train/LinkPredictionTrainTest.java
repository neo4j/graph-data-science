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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfigImpl;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.HadamardFeatureStep;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LinkPredictionTrainTest {

    static String NODES = "(a:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
                          "(b:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
                          "(c:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
                          "(d:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
                          "(e:N {noise: 42, z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
                          "(f:N {noise: 42, z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
                          "(g:N {noise: 42, z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
                          "(h:N {noise: 42, z: 200, array: [-1.0,-2.0,3.0,4.0,5.0]}), " +
                          "(i:N {noise: 42, z: 200, array: [-1.0,-2.0,3.0,4.0,5.0]}), " +
                          "(j:N {noise: 42, z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
                          "(k:N {noise: 42, z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
                          "(l:N {noise: 42, z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
                          "(m:N {noise: 42, z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +
                          "(n:N {noise: 42, z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +
                          "(o:N {noise: 42, z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), ";

    @GdlGraph(graphNamePrefix = "train")
    static String GRAPH =
        "CREATE " +
        NODES +
        "(a)-[:REL {label: 1.0}]->(b), " +
        "(a)-[:REL {label: 1.0}]->(c), " +
        "(a)-[:REL {label: 0.0}]->(e), " +
        "(a)-[:REL {label: 0.0}]->(h), " +
        "(a)-[:REL {label: 0.0}]->(i), " +
        "(a)-[:REL {label: 0.0}]->(i), " +
        "(b)-[:REL {label: 1.0}]->(c), " +
        "(b)-[:REL {label: 0.0}]->(f), " +
        "(b)-[:REL {label: 0.0}]->(g), " +
        "(b)-[:REL {label: 0.0}]->(n), " +
        "(b)-[:REL {label: 0.0}]->(o), " +
        "(c)-[:REL {label: 1.0}]->(d), " +
        "(c)-[:REL {label: 0.0}]->(h), " +
        "(c)-[:REL {label: 0.0}]->(l), " +
        "(e)-[:REL {label: 1.0}]->(f), " +
        "(e)-[:REL {label: 0.0}]->(a), " +
        "(f)-[:REL {label: 1.0}]->(g), " +
        "(f)-[:REL {label: 0.0}]->(o), " +
        "(h)-[:REL {label: 1.0}]->(i), " +
        "(j)-[:REL {label: 1.0}]->(k), " +
        "(k)-[:REL {label: 1.0}]->(l), " +
        "(m)-[:REL {label: 1.0}]->(n), " +
        "(n)-[:REL {label: 1.0}]->(o) ";

    @GdlGraph(graphNamePrefix = "validation")
    static String VALIDATION =
        "CREATE " +
       NODES +
        "(a)-[:REL {label: 1.0}]->(d), " +
        "(a)-[:REL {label: 0.0}]->(o), " +
        "(b)-[:REL {label: 1.0}]->(d), " +
        "(b)-[:REL {label: 0.0}]->(i), " +
        "(e)-[:REL {label: 1.0}]->(g), " +
        "(e)-[:REL {label: 0.0}]->(k), " +
        "(j)-[:REL {label: 1.0}]->(l), " +
        "(j)-[:REL {label: 0.0}]->(a), " +
        "(m)-[:REL {label: 1.0}]->(o), " +
        "(m)-[:REL {label: 0.0}]->(b), ";

    @Inject
    Graph trainGraph;
    @Inject
    Graph validationGraph;

    static Stream<Arguments> paramsForEstimationsWithSplitConfigs() {
        return Stream.of(
            Arguments.of(
                "Default",
                LinkPredictionSplitConfigImpl.builder().testFraction(0.1).trainFraction(0.1).validationFolds(2).build(),
                108_088,
                3_138_248
            ),
            Arguments.of(
                "Higher test-set",
                LinkPredictionSplitConfigImpl.builder().testFraction(0.6).trainFraction(0.1).validationFolds(2).build(),
                192_248,
                6_515_208
            ),
            Arguments.of(
                "Higher train-set",
                LinkPredictionSplitConfigImpl.builder().testFraction(0.1).trainFraction(0.6).validationFolds(2).build(),
                324_088,
                10_410_248
            ),
            Arguments.of(
                "Higher validation folds",
                LinkPredictionSplitConfigImpl.builder().testFraction(0.1).trainFraction(0.6).validationFolds(5).build(),
                376_264,
                10_462_424
            )
        );
    }

    static Stream<Arguments> paramsForEstimationsWithParamSpace() {
        var llrConfigs = Stream.of(
                Map.<String, Object>of("batchSize", 10),
                Map.<String, Object>of("batchSize", 100L)
            )
            .map(LinkLogisticRegressionTrainConfig::of)
            .collect(Collectors.toList());

        return Stream.of(
            Arguments.of("LLR batchSize 10", List.of(llrConfigs.get(0)), 56360, 1675320),
            Arguments.of("LLR batchSize 100", List.of(llrConfigs.get(1)), 111080, 3141240),
            Arguments.of("LLR batchSize 10,100", llrConfigs, 111176, 3141336)
        );
    }

    @Test
    void trainsAModel() {
        String modelName = "model";

        LinkPredictionTrainConfig trainConfig = trainingConfig(modelName);

        var result = runLinkPrediction(trainConfig);

        var actualModel = result.model();

        assertThat(actualModel.name()).isEqualTo(modelName);
        assertThat(actualModel.algoType()).isEqualTo(LinkPredictionTrain.MODEL_TYPE);
        assertThat(actualModel.trainConfig()).isEqualTo(trainConfig);
        // length of the linkFeatures
        assertThat(actualModel.data().weights().data().totalSize()).isEqualTo(7);

        var customInfo = actualModel.customInfo();
        assertThat(result.modelSelectionStatistics().validationStats().get(LinkMetric.AUCPR))
            .satisfies(scores ->
                assertThat(scores.get(0).avg()).isNotCloseTo(scores.get(1).avg(), Percentage.withPercentage(0.2))
            );

        assertThat(customInfo.bestParameters())
            .usingRecursiveComparison()
            .isEqualTo(LinkLogisticRegressionTrainConfig.of(Map.of("penalty", 1)));
    }

    @Test
    void seededTrain() {
        String modelName = "model";

        LinkPredictionTrainConfig trainConfig = trainingConfig(modelName);

        var modelData = runLinkPrediction(trainConfig).model().data();
        var modelDataRepeated = runLinkPrediction(trainConfig).model().data();

        var modelWeights = modelData.weights().data();
        var modelBias = modelData.bias().get().data().value();
        var modelWeightsRepeated = modelDataRepeated.weights().data();
        var modelBiasRepeated = modelDataRepeated.bias().get().data().value();

        assertThat(modelWeights).matches(modelWeightsRepeated::equals);
        assertThat(modelBias).isEqualTo(modelBiasRepeated);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "  10,   10,  65_256, 1_692_056",
        "  10,  100,  69_608, 1_829_688",
        "  10, 1000, 111_080, 3_141_240",
        "1000, 1000, 111_080, 3_141_240"
    })
    void estimateWithDifferentGraphSizes(int nodeCount, int relationshipCount, int expectedMinEstimation, int expectedMaxEstimation) {
        var trainConfig = LinkPredictionTrainConfig
            .builder()
            .modelName("DUMMY")
            .graphName("DUMMY")
            .pipeline("DUMMY")
            .build();

        var pipeline = new LinkPredictionPipeline();
        var graphDim = GraphDimensions.of(nodeCount, relationshipCount);
        var actualEstimation = LinkPredictionTrain.estimate(pipeline, trainConfig).estimate(graphDim, trainConfig.concurrency());

        assertThat(actualEstimation.memoryUsage()).isEqualTo(MemoryRange.of(expectedMinEstimation, expectedMaxEstimation));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("paramsForEstimationsWithSplitConfigs")
    void estimateWithDifferentSplits(String desc, LinkPredictionSplitConfig splitConfig, int expectedMinEstimation, int expectedMaxEstimation) {
        var trainConfig = LinkPredictionTrainConfig
            .builder()
            .modelName("DUMMY")
            .graphName("DUMMY")
            .pipeline("DUMMY")
            .build();

        var pipeline = new LinkPredictionPipeline();
        pipeline.setSplitConfig(splitConfig);
        var graphDim = GraphDimensions.of(100, 1_000);
        var actualEstimation = LinkPredictionTrain.estimate(pipeline, trainConfig).estimate(graphDim, trainConfig.concurrency());

        assertThat(actualEstimation.memoryUsage()).isEqualTo(MemoryRange.of(expectedMinEstimation, expectedMaxEstimation));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("paramsForEstimationsWithParamSpace")
    void estimateWithParameterSpace(String desc, List<LinkLogisticRegressionTrainConfig> parameterSpace, int expectedMinEstimation, int expectedMaxEstimation) {
        var trainConfig = LinkPredictionTrainConfig
            .builder()
            .modelName("DUMMY")
            .graphName("DUMMY")
            .pipeline("DUMMY")
            .build();

        var pipeline = new LinkPredictionPipeline();
        pipeline.setTrainingParameterSpace(parameterSpace);
        var graphDim = GraphDimensions.of(100, 1_000);
        var actualEstimation = LinkPredictionTrain.estimate(pipeline, trainConfig).estimate(graphDim, trainConfig.concurrency());

        System.out.println("actualEstimation.memoryUsage().min = " + actualEstimation.memoryUsage().min);
        System.out.println("actualEstimation.memoryUsage().max = " + actualEstimation.memoryUsage().max);

        assertThat(actualEstimation.memoryUsage()).isEqualTo(MemoryRange.of(expectedMinEstimation, expectedMaxEstimation));
    }

    @ParameterizedTest
    @CsvSource(value = {
        "  1,  63_776, 1_894_416",
        "  2,  79_544, 2_310_024",
        "  4, 111_080, 3_141_240",
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
        var graphDim = GraphDimensions.of(100, 1_000);
        var actualEstimation = LinkPredictionTrain.estimate(pipeline, trainConfig).estimate(graphDim, trainConfig.concurrency());

        assertThat(actualEstimation.memoryUsage()).isEqualTo(MemoryRange.of(expectedMinEstimation, expectedMaxEstimation));
    }


    private LinkPredictionPipeline linkPredictionPipeline() {
        LinkPredictionPipeline pipeline = new LinkPredictionPipeline();

        pipeline.setSplitConfig(LinkPredictionSplitConfig.builder()
            .validationFolds(2)
            .negativeSamplingRatio(1)
            .trainFraction(0.5)
            .testFraction(0.5)
            .build());

        pipeline.setTrainingParameterSpace(List.of(
            LinkLogisticRegressionTrainConfig.of(Map.of("penalty", 1000000)),
            LinkLogisticRegressionTrainConfig.of(Map.of("penalty", 1))
        ));

        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("noise", "z", "array")));
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
            validationGraph,
            linkPredictionPipeline(),
            trainConfig,
            ProgressTracker.NULL_TRACKER
        );

        return linkPredictionTrain.compute();
    }
}
