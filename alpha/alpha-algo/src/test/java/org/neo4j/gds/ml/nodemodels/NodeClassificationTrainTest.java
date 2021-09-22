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
package org.neo4j.gds.ml.nodemodels;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.TestLog;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;
import org.neo4j.gds.ml.nodemodels.metrics.AllClassMetric;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;
import org.neo4j.logging.NullLog;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestLog.INFO;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

@GdlExtension
class NodeClassificationTrainTest {

    // TODO validation
    // at least one config

    @GdlGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (:N {bananas: 100.0, arrayProperty: [1.2, 1.2], a: 1.2, b: 1.2, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [2.8, 2.5], a: 2.8, b: 2.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [3.3, 0.5], a: 3.3, b: 0.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.0, 0.5], a: 1.0, b: 0.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.32, 0.5], a: 1.32, b: 0.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.3, 1.5], a: 1.3, b: 1.5, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [5.3, 10.5], a: 5.3, b: 10.5, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.3, 2.5], a: 1.3, b: 2.5, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [0.0, 66.8], a: 0.0, b: 66.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [0.1, 2.8], a: 0.1, b: 2.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [0.66, 2.8], a: 0.66, b: 2.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [2.0, 10.8], a: 2.0, b: 10.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [5.0, 7.8], a: 5.0, b: 7.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [4.0, 5.8], a: 4.0, b: 5.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.0, 0.9], a: 1.0, b: 0.9, t: 1})";

    @Inject
    TestGraph graph;

    @ParameterizedTest
    @MethodSource("metricArguments")
    void selectsTheBestModel(MetricSpecification metricSpecification) {

        var metric = metricSpecification.createMetrics(List.of()).findFirst().get();

        Map<String, Object> model1 = Map.of("penalty", 1, "maxEpochs", 1);
        Map<String, Object> model2 = Map.of("penalty", 1, "maxEpochs", 10000, "tolerance", 1e-5);

        var config = createConfig(
            List.of(model1, model2),
            "model",
            List.of("a", "b"),
            metricSpecification,
            1L
        );
        var expectedWinner = config.paramsConfig().stream().filter(c -> c.maxEpochs() == 10000).findFirst().get();

        var ncTrain = NodeClassificationTrain.create(
            graph,
            config,
            AllocationTracker.empty(),
            ProgressTracker.NULL_TRACKER
        );

        var model = ncTrain.compute();

        var customInfo = model.customInfo();
        var validationScores = customInfo.metrics().get(metric).validation();

        assertThat(validationScores).hasSize(2);
        var actualWinnerParams = customInfo.bestParameters();
        assertThat(actualWinnerParams).isEqualTo(expectedWinner);
        double model1Score = validationScores.get(0).avg();
        double model2Score = validationScores.get(1).avg();
        assertThat(model1Score).isNotCloseTo(model2Score, Percentage.withPercentage(0.2));
    }

    @GdsEditionTest(Edition.EE)
    @ParameterizedTest
    @MethodSource("metricArguments")
    void shouldProduceDifferentMetricsForDifferentTrainings(MetricSpecification metricSpecification) {

        var metric = metricSpecification.createMetrics(List.of()).findFirst().get();

        var modelCandidates = List.of(
            Map.<String, Object>of("penalty", 0.0625, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 0.125, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 0.25, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 0.5, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 1.0, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 2.0, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 4.0, "maxEpochs", 1000)
        );

        var bananasConfig = createConfig(
            modelCandidates,
            "bananasModel",
            List.of("bananas"),
            metricSpecification,
            1337L
        );
        var bananasTrain = NodeClassificationTrain.create(
            graph,
            bananasConfig,
            AllocationTracker.empty(),
            ProgressTracker.NULL_TRACKER
        );

        var arrayPropertyConfig = createConfig(
            modelCandidates,
            "arrayPropertyModel",
            List.of("arrayProperty"),
            metricSpecification,
            42L
        );
        var arrayPropertyTrain = NodeClassificationTrain.create(
            graph,
            arrayPropertyConfig,
            AllocationTracker.empty(),
            ProgressTracker.NULL_TRACKER
        );

        var bananasModel = bananasTrain.compute();
        var arrayPropertyModel = arrayPropertyTrain.compute();

        assertThat(arrayPropertyModel)
            .usingRecursiveComparison()
            .withFailMessage("The trained models are exactly the same instance!")
            .isNotSameAs(bananasModel);

        assertThat(arrayPropertyModel.data())
            .usingRecursiveComparison()
            .withFailMessage("Should not produce the same trained `data`!")
            .isNotEqualTo(bananasModel.data());

        var bananasCustomInfo = bananasModel.customInfo();
        var bananasValidationScore = bananasCustomInfo.metrics().get(metric);

        var arrayPropertyCustomInfo = arrayPropertyModel.customInfo();
        var arrayPropertyValidationScores = arrayPropertyCustomInfo.metrics().get(metric);

        assertThat(arrayPropertyValidationScores)
            .usingRecursiveComparison()
            .isNotSameAs(bananasValidationScore)
            .isNotEqualTo(bananasValidationScore);
    }

    @GdsEditionTest(Edition.EE)
    @ParameterizedTest
    @MethodSource("metricArguments")
    void shouldLogProgress(MetricSpecification metricSpecification) {
        var modelCandidates = List.of(
            Map.<String, Object>of("penalty", 0.0625, "maxEpochs", 100),
            Map.<String, Object>of("penalty", 0.125, "maxEpochs", 100)
        );
        var log = new TestLog();

        var config = createConfig(
            modelCandidates,
            "bananasModel",
            List.of("bananas"),
            metricSpecification,
            42L
        );
        var factory = new NodeClassificationTrainAlgorithmFactory();
        var algorithm = factory.build(
            graph,
            config,
            AllocationTracker.empty(),
            log,
            EmptyTaskRegistryFactory.INSTANCE
        );

        algorithm.compute();

        assertThat(log.getMessages(INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "NCTrain :: Start",
                "NCTrain :: ShuffleAndSplit :: Start",
                "NCTrain :: ShuffleAndSplit 100%",
                "NCTrain :: ShuffleAndSplit :: Finished",
                "NCTrain :: SelectBestModel :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 1 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 1 :: Loss: 0.6376390741595698",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 1 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 1 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 2 :: Loss: 0.5922156656203446",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 2 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 3 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 3 :: Loss: 0.556577259895119",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 3 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 3 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 4 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 4 :: Loss: 0.5302477405631714",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 4 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 4 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 5 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 5 :: Loss: 0.5126085921201048",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 5 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 5 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 6 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 6 :: Loss: 0.5029395110823727",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 6 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 6 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 7 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 7 :: Loss: 0.5004365364962841",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 7 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 7 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 8 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 8 :: Loss: 0.5031678097691253",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 8 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 8 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: converged after 8 epochs." +
                " Initial loss: 0.6931471805599457, Last loss: 0.5031678097691253.",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Evaluate :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Evaluate 50%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Evaluate 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Evaluate :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 1 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 1 :: Loss: 0.678039074072859",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 1 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 1 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 2 :: Loss: 0.6730121557387895",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 2 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 3 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 3 :: Loss: 0.675865964351673",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 3 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 3 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: converged after 3 epochs." +
                " Initial loss: 0.6931471805599457, Last loss: 0.675865964351673.",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Evaluate :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Evaluate 50%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Evaluate 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Evaluate :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 1 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 1 :: Loss: 0.6376391158262363",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 1 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 1 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 2 :: Loss: 0.5922158322870106",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 2 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 3 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 3 :: Loss: 0.5565776348951175",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 3 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 3 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 4 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 4 :: Loss: 0.5302484072298354",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 4 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 4 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 5 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 5 :: Loss: 0.5126096337867673",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 5 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 5 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 6 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 6 :: Loss: 0.5029410110823667",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 6 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 6 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 7 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 7 :: Loss: 0.5004385557772911",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 7 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 7 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 8 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 8 :: Loss: 0.503170225499523",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 8 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 8 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: converged after 8 epochs." +
                " Initial loss: 0.6931471805599457, Last loss: 0.503170225499523.",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Evaluate :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Evaluate 50%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Evaluate 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Evaluate :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 1 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 1 :: Loss: 0.6780391157395256",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 1 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 1 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 2 :: Loss: 0.6730123223940423",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 2 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 3 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 3 :: Loss: 0.675866228402635",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 3 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 3 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: converged after 3 epochs." +
                " Initial loss: 0.6931471805599457, Last loss: 0.675866228402635.",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Evaluate :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Evaluate 50%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Evaluate 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Evaluate :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Finished",
                "NCTrain :: SelectBestModel :: Finished",
                "NCTrain :: TrainSelectedOnRemainder :: Start",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 1 :: Start",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 1 :: Loss: 0.6578391157845587",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 1 100%",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 1 :: Finished",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 2 :: Start",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 2 :: Loss: 0.6326157963772417",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 2 100%",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 2 :: Finished",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 3 :: Start",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 3 :: Loss: 0.6171746769535039",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 3 100%",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 3 :: Finished",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 4 :: Start",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 4 :: Loss: 0.6110320544824134",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 4 100%",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 4 :: Finished",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 5 :: Start",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 5 :: Loss: 0.6128677266585574",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 5 100%",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 5 :: Finished",
                "NCTrain :: TrainSelectedOnRemainder :: converged after 5 epochs." +
                " Initial loss: 0.6931471805599456, Last loss: 0.6128677266585574.",
                "NCTrain :: TrainSelectedOnRemainder :: Finished",
                "NCTrain :: EvaluateSelectedModel :: Start",
                "NCTrain :: EvaluateSelectedModel 33%",
                "NCTrain :: EvaluateSelectedModel 100%",
                "NCTrain :: EvaluateSelectedModel :: Finished",
                "NCTrain :: RetrainSelectedModel :: Start",
                "NCTrain :: RetrainSelectedModel :: Epoch 1 :: Start",
                "NCTrain :: RetrainSelectedModel :: Epoch 1 :: Loss: 0.6645724907702185",
                "NCTrain :: RetrainSelectedModel :: Epoch 1 100%",
                "NCTrain :: RetrainSelectedModel :: Epoch 1 :: Finished",
                "NCTrain :: RetrainSelectedModel :: Epoch 2 :: Start",
                "NCTrain :: RetrainSelectedModel :: Epoch 2 :: Loss: 0.646082386126392",
                "NCTrain :: RetrainSelectedModel :: Epoch 2 100%",
                "NCTrain :: RetrainSelectedModel :: Epoch 2 :: Finished",
                "NCTrain :: RetrainSelectedModel :: Epoch 3 :: Start",
                "NCTrain :: RetrainSelectedModel :: Epoch 3 :: Loss: 0.6373713899994502",
                "NCTrain :: RetrainSelectedModel :: Epoch 3 100%",
                "NCTrain :: RetrainSelectedModel :: Epoch 3 :: Finished",
                "NCTrain :: RetrainSelectedModel :: Epoch 4 :: Start",
                "NCTrain :: RetrainSelectedModel :: Epoch 4 :: Loss: 0.637609501031641",
                "NCTrain :: RetrainSelectedModel :: Epoch 4 100%",
                "NCTrain :: RetrainSelectedModel :: Epoch 4 :: Finished",
                "NCTrain :: RetrainSelectedModel :: converged after 4 epochs." +
                " Initial loss: 0.6931471805599455, Last loss: 0.637609501031641.",
                "NCTrain :: RetrainSelectedModel :: Finished",
                "NCTrain :: Finished"
            );
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void seededNodeClassification(int concurrency) {
        var config = ImmutableNodeClassificationTrainConfig.builder()
            .modelName("model")
            .featureProperties(List.of("bananas"))
            .holdoutFraction(0.33)
            .validationFolds(2)
            .randomSeed(42L)
            .targetProperty("t")
            .metrics(List.of(MetricSpecification.parse("Accuracy")))
            .params(List.of(Map.of("penalty", 0.0625, "maxEpochs", 100, "batchSize", 1)))
            .concurrency(concurrency)
            .build();

        Supplier<NodeClassificationTrain> algoSupplier = () -> new NodeClassificationTrainAlgorithmFactory().build(
            graph,
            config,
            AllocationTracker.empty(),
            NullLog.getInstance(),
            EmptyTaskRegistryFactory.INSTANCE
        );

        var firstResult = algoSupplier.get().compute();
        var secondResult = algoSupplier.get().compute();

        assertThat(firstResult.data().weights().data())
            .matches(matrix -> matrix.equals(secondResult.data().weights().data(), 1e-10));
    }

    private NodeClassificationTrainConfig createConfig(
        Iterable<Map<String, Object>> modelCandidates,
        String modelName,
        Iterable<String> featureProperties,
        MetricSpecification metricSpecification,
        long randomSeed
    ) {
        return ImmutableNodeClassificationTrainConfig.builder()
            .modelName(modelName)
            .featureProperties(featureProperties)
            .holdoutFraction(0.33)
            .validationFolds(2)
            .concurrency(1)
            .randomSeed(randomSeed)
            .targetProperty("t")
            .metrics(List.of(metricSpecification))
            .params(modelCandidates)
            .build();
    }

    static Stream<Arguments> metricArguments() {
        var singleClassMetrics = Stream.of(Arguments.arguments(MetricSpecification.parse("F1(class=1)")));
        var allClassMetrics = Arrays
            .stream(AllClassMetric.values())
            .map(AllClassMetric::name)
            .map(MetricSpecification::parse)
            .map(Arguments::of);
        return Stream.concat(singleClassMetrics, allClassMetrics);
    }
}
