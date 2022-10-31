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
import org.neo4j.gds.ml.models.mlp.MLPClassifierTrainConfig;
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
        "  (a1:N  {arrayProperty: [1.2, 0.0, 0.0],  moreOnes: 0, multiClass: 0})" +
        ", (a2:N  {arrayProperty: [2.8, 0.0, 0.0],  moreOnes: 0, multiClass: 0})" +
        ", (a3:N  {arrayProperty: [3.3, 0.0, 0.0],  moreOnes: 0, multiClass: 0})" +
        ", (a4:N  {arrayProperty: [1.0, 0.0, 0.0],  moreOnes: 0, multiClass: 0})" +
        ", (a5:N  {arrayProperty: [1.32, 0.0, 0.0], moreOnes: 0, multiClass: 0})" +
        ", (a6:N  {arrayProperty: [1.3, 1.5, 0.0],  moreOnes: 0, multiClass: 2})" +
        ", (a7:N  {arrayProperty: [5.3, 10.5, 0.0], moreOnes: 1, multiClass: 1})" +
        ", (a8:N  {arrayProperty: [1.3, 2.5, 0.0],  moreOnes: 1, multiClass: 2})" +
        ", (a9:N  {arrayProperty: [0.0, 66.8, 0.0], moreOnes: 1, multiClass: 2})" +
        ", (a10:N {arrayProperty: [0.1, 2.8, 0.0],  moreOnes: 1, multiClass: 2})" +
        ", (a11:N {arrayProperty: [0.66, 2.8, 9.92], moreOnes: 1, multiClass: 2})" +
        ", (a12:N {arrayProperty: [2.0, 10.8, 2.22], moreOnes: 1, multiClass: 2})" +
        ", (a13:N {arrayProperty: [5.0, 7.8, 19.92],  moreOnes: 1, multiClass: 2})" +
        ", (a14:N {arrayProperty: [4.0, 5.8, 8.2],  moreOnes: 1, multiClass: 2})" +
        ", (a15:N {arrayProperty: [1.0, 0.9, 1.0],  moreOnes: 1, multiClass: 2})";

    @Inject
    private GraphStore graphStore;

    @Test
    void focalLossImprovesDifficultClassPredictionsLR() {
        var LRResultNoFocus = trainLRWithFocus(0, "multiClass");
        var LRResultFocus = trainLRWithFocus(5, "multiClass");

        double[] LPHardPredictionNoFocus = LRResultNoFocus.classifier().predictProbabilities(new double[]{5.3, 10.5, 0.0});
        double[] LRHardPredictionFocus = LRResultFocus.classifier().predictProbabilities(new double[]{5.3, 10.5, 0.0});

        // the example data point (5.3, 10.5, 0.0) (a7 from train set above) is hard to classify (the only 1-class example)
        // applying focus makes the model more probable to predict the correct class (harder)
        assertThat(LRHardPredictionFocus[1]).isGreaterThan(LPHardPredictionNoFocus[1]);
    }

    @Test
    void focalLossImprovesDifficultClassPredictionsMLP() {
        var MLPResultNoFocus = trainMLPWithFocus(0, "multiClass");
        var MLPResultFocus = trainMLPWithFocus(5, "multiClass");

        double[] MLPHardPredictionNoFocus = MLPResultNoFocus.classifier().predictProbabilities(new double[]{5.3, 10.5, 0.0});
        double[] MLPHardPredictionFocus = MLPResultFocus.classifier().predictProbabilities(new double[]{5.3, 10.5, 0.0});

        assertThat(MLPHardPredictionFocus[1]).isGreaterThan(MLPHardPredictionNoFocus[1]);
    }

    @Test
    void focalLossImprovesMinorityClassPredictionsLR() {
        var LRResultNoFocus = trainLRWithFocus(0, "moreOnes");
        var LRResultFocus = trainLRWithFocus(5, "moreOnes");

        double[] LRHardPredictionNoFocus = LRResultNoFocus.classifier().predictProbabilities(new double[]{1.3, 1.5, 0.0});
        double[] LRHardPredictionFocus = LRResultFocus.classifier().predictProbabilities(new double[]{1.3, 1.5, 0.0});

        // the example data point (1.3, 1.5, 0.0) (a6 from train set above) is hard to classify
        // applying focus makes the model more probable to predict the correct class (harder)
        assertThat(LRHardPredictionFocus[0]).isGreaterThan(LRHardPredictionNoFocus[0]);

        // applying focus
        assertThat(LRResultFocus.zeroClassPrecision()).isGreaterThanOrEqualTo(LRResultNoFocus.zeroClassPrecision());
    }

    @Test
    void focalLossImprovesMinorityClassPredictionsMLP() {
        var MLPResultNoFocus = trainMLPWithFocus(0, "moreOnes");
        var MLPResultFocus = trainMLPWithFocus(5, "moreOnes");

        double[] MLPHardPredictionNoFocus = MLPResultNoFocus.classifier().predictProbabilities(new double[]{1.3, 1.5, 0.0});
        //however for MLP, it is powerful enough to classify a6, and it is not a hard result
        assertThat(MLPHardPredictionNoFocus[0]).isGreaterThan(0.5);

        // applying focus
        assertThat(MLPResultFocus.zeroClassPrecision()).isGreaterThanOrEqualTo(MLPResultNoFocus.zeroClassPrecision());
    }

    private ClassifierAndTestMetric trainLRWithFocus(
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

        return buildClassifierAndTestMetric(pipeline, targetProperty);
    }

    private ClassifierAndTestMetric trainMLPWithFocus(
        double focusWeight, String targetProperty
    ) {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.addFeatureStep(NodeFeatureStep.of("arrayProperty"));

        pipeline.addTrainerConfig(MLPClassifierTrainConfig.of(Map.of(
            "penalty", 0.1,
            "patience", 10,
            "tolerance", 0.0001,
            "focusWeight", focusWeight,
            "hiddenLayerSizes", List.of(12,4)
        )));

        return buildClassifierAndTestMetric(pipeline, targetProperty);
    }

    private ClassifierAndTestMetric buildClassifierAndTestMetric(NodeClassificationTrainingPipeline pipeline, String targetProperty) {
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
