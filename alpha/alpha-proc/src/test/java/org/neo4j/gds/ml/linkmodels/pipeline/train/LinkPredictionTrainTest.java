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
package org.neo4j.gds.ml.linkmodels.pipeline.train;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionModelInfo;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipeline;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;

import java.util.List;
import java.util.Map;

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

    @Test
    void trainsAModel() {
        String modelName = "model";


        LinkPredictionTrainConfig trainConfig = trainingConfig(modelName);

        var actualModel = runLinkPrediction(trainConfig);

        assertThat(actualModel.name()).isEqualTo(modelName);
        assertThat(actualModel.algoType()).isEqualTo(LinkPredictionTrain.MODEL_TYPE);
        assertThat(actualModel.trainConfig()).isEqualTo(trainConfig);
        // length of the linkFeatures
        assertThat(actualModel.data().weights().data().totalSize()).isEqualTo(7);

        var customInfo = actualModel.customInfo();
        assertThat(customInfo.metrics().get(LinkMetric.AUCPR).validation())
            .hasSize(2)
            .satisfies(scores ->
                assertThat(scores.get(0).avg()).isNotCloseTo(scores.get(1).avg(), Percentage.withPercentage(0.2))
            );

        assertThat(customInfo.bestParameters())
            .usingRecursiveComparison()
            .isEqualTo(LinkLogisticRegressionTrainConfig.of(4, Map.of("penalty", 1)));
    }

    @Test
    void seededTrain() {
        String modelName = "model";

        LinkPredictionTrainConfig trainConfig = trainingConfig(modelName);

        var modelData = runLinkPrediction(trainConfig).data();
        var modelDataRepeated = runLinkPrediction(trainConfig).data();

        var modelWeights = modelData.weights().data();
        var modelBias = modelData.bias().get().data().value();
        var modelWeightsRepeated = modelDataRepeated.weights().data();
        var modelBiasRepeated = modelDataRepeated.bias().get().data().value();

        assertThat(modelWeights).matches(modelWeightsRepeated::equals);
        assertThat(modelBias).isEqualTo(modelBiasRepeated);
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
            LinkLogisticRegressionTrainConfig.of(4, Map.of("penalty", 1000000)),
            LinkLogisticRegressionTrainConfig.of(4, Map.of("penalty", 1))
        ));

        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("noise", "z", "array")));
        return pipeline;
    }

    private LinkPredictionTrainConfig trainingConfig(String modelName) {
        return LinkPredictionTrainConfig
            .builder()
            .graphName("graph")
            .modelName(modelName)
            .pipeline("DUMMY")
            .negativeClassWeight(1)
            .randomSeed(1337L)
            .build();
    }

    private Model<LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo> runLinkPrediction(
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
