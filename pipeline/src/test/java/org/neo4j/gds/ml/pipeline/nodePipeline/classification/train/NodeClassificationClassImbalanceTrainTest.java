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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureProducer;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class NodeClassificationClassImbalanceTrainTest {

    @GdlGraph
    private static final String GRAPH =
        "  (a1:N  {arrayProperty: [1.2, 0.0, 0.0],  moreOnes: 0, perfectBalance: 0})" +
        ", (a2:N  {arrayProperty: [2.8, 0.0, 0.0],  moreOnes: 0, perfectBalance: 0})" +
        ", (a3:N  {arrayProperty: [3.3, 0.0, 0.0],  moreOnes: 0, perfectBalance: 0})" +
        ", (a4:N  {arrayProperty: [1.0, 0.0, 0.0],  moreOnes: 0, perfectBalance: 0})" +
        ", (a5:N  {arrayProperty: [1.32, 0.0, 0.0], moreOnes: 0, perfectBalance: 0})" +
        ", (a6:N  {arrayProperty: [1.3, 1.5, 0.0],  moreOnes: 0, perfectBalance: 1})" +
        ", (a7:N  {arrayProperty: [5.3, 10.5, 0.0], moreOnes: 1, perfectBalance: 1})" +
        ", (a8:N  {arrayProperty: [1.3, 2.5, 0.0],  moreOnes: 1, perfectBalance: 1})" +
        ", (a9:N  {arrayProperty: [0.0, 66.8, 0.0], moreOnes: 1, perfectBalance: 1})" +
        ", (a10:N {arrayProperty: [0.1, 2.8, 0.0],  moreOnes: 1, perfectBalance: 1})" +
        ", (a11:N {arrayProperty: [0.66, 2.8, 9.92], moreOnes: 1, perfectBalance: 2})" +
        ", (a12:N {arrayProperty: [2.0, 10.8, 2.22], moreOnes: 1, perfectBalance: 2})" +
        ", (a13:N {arrayProperty: [5.0, 7.8, 19.92],  moreOnes: 1, perfectBalance: 2})" +
        ", (a14:N {arrayProperty: [4.0, 5.8, 8.2],  moreOnes: 1, perfectBalance: 2})" +
        ", (a15:N {arrayProperty: [1.0, 0.9, 1.0],  moreOnes: 1, perfectBalance: 2})";

    @Inject
    private GraphStore graphStore;

    @Test
    void focalLossImprovesDifficultClassPredictions() {
        var resultNoFocus = trainWithFocus(0, "perfectBalance");
        var resultFocus = trainWithFocus(5, "perfectBalance");

        double[] hardPredictionNoFocus = resultNoFocus.classifier().predictProbabilities(new double[]{1.0, 0.1, 0.1});
        double[] hardPredictionFocus = resultFocus.classifier().predictProbabilities(new double[]{1.0, 0.1, 0.1});

        // the example data point (1, 0.1, 0.1) is very similar to classes 0 and 1 in the train set
        // applying focus makes the model more probable to predict the correct class (harder)
        assertThat(hardPredictionFocus[2]).isGreaterThan(hardPredictionNoFocus[2]);
    }

    @Test
    void focalLossImprovesMinorityClassPredictions() {
        var resultNoFocus = trainWithFocus(0, "moreOnes");
        var resultFocus = trainWithFocus(5, "moreOnes");

        double[] hardPredictionNoFocus = resultNoFocus.classifier().predictProbabilities(new double[]{1.3, 1.5, 0.0});
        double[] hardPredictionFocus = resultFocus.classifier().predictProbabilities(new double[]{1.3, 1.5, 0.0});

        // the example data point (1.3, 1.5, 0.0) (a6 from train set above) is hard to classify
        // applying focus makes the model more probable to predict the correct class (harder)
        assertThat(hardPredictionFocus[0]).isGreaterThan(hardPredictionNoFocus[0]);

        // applying focus
        assertThat(resultFocus.zeroClassPrecision()).isGreaterThanOrEqualTo(resultNoFocus.zeroClassPrecision());
    }

    private ClassifierAndTestMetric trainWithFocus(
        double focusWeight, String targetProperty
    ) {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.addFeatureStep(NodeFeatureStep.of("arrayProperty"));

        pipeline.addTrainerConfig(LogisticRegressionTrainConfig.of(Map.of(
            "penalty", 0.1,
            "patience", 10,
            "tolerance", 0.0001,
            "focusWeight", focusWeight
        )));

        var trainConfig = NodeClassificationPipelineTrainConfigImpl
            .builder()
            .pipeline("PIPELINE")
            .graphName("GRAPH")
            .modelUser("DUMMY")
            .modelName("model")
            .concurrency(1)
            .randomSeed(-1L)
            .targetProperty(targetProperty)
            .metrics(List.of(
                ClassificationMetricSpecification.Parser.parse("PRECISION(class=0)")
            ))
            .build();

        var result = NodeClassificationTrain.create(
            graphStore,
            pipeline,
            trainConfig,
            NodeFeatureProducer.create(
                graphStore,
                trainConfig,
                ExecutionContext.EMPTY,
                ProgressTracker.NULL_TRACKER
            ),
            ProgressTracker.NULL_TRACKER
        ).run();

        return ImmutableClassifierAndTestMetric
            .builder()
            .classifier(result.classifier())
            .zeroClassPrecision(result
                .trainingStatistics()
                .winningModelTestMetrics()
                .values()
                .stream()
                .findFirst()
                .orElseThrow())
            .build();
    }

    @ValueClass
    interface ClassifierAndTestMetric {
        Classifier classifier();
        double zeroClassPrecision();
    }
}
