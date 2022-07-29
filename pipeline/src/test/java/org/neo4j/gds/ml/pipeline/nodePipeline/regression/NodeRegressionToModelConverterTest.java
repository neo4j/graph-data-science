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
package org.neo4j.gds.ml.pipeline.nodePipeline.regression;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.ml.metrics.ModelCandidateStats;
import org.neo4j.gds.ml.metrics.ModelStats;
import org.neo4j.gds.ml.metrics.regression.RegressionMetrics;
import org.neo4j.gds.ml.models.Regressor;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfigImpl;
import org.neo4j.gds.ml.training.TrainingStatistics;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NodeRegressionToModelConverterTest {

    @Test
    void convertTrainResult() {
        var graphStore = GdlFactory.of("({age: 23, height: 42})-->({age: 22, height: 99})").build();

        var pipeline = new NodeRegressionTrainingPipeline();

        pipeline.nodePropertySteps().add(NodePropertyStepFactory.createNodePropertyStep(
            "testProc",
            Map.of("mutateProperty", "pr")
        ));

        pipeline.addFeatureStep(NodeFeatureStep.of("age"));
        pipeline.addFeatureStep(NodeFeatureStep.of("height"));
        pipeline.addFeatureStep(NodeFeatureStep.of("pr"));

        LinearRegressionTrainConfig modelCandidate = LinearRegressionTrainConfig.DEFAULT;
        pipeline.addTrainerConfig(modelCandidate);

        pipeline.setSplitConfig(NodePropertyPredictionSplitConfigImpl.builder()
            .testFraction(0.3)
            .validationFolds(2)
            .build()
        );

        RegressionMetrics evaluationMetric = RegressionMetrics.MEAN_ABSOLUTE_ERROR;
        var config = NodeRegressionPipelineTrainConfigImpl.builder()
            .pipeline("MY_PIPE")
            .graphName("GRAPH")
            .modelUser("MY USER")
            .modelName("model")
            .concurrency(1)
            .randomSeed(1L)
            .targetProperty("t")
            .metrics(List.of(evaluationMetric))
            .build();

        var regressor = new Regressor() {
            @Override
            public double predict(double[] features) {
                return 0;
            }

            @Override
            public RegressorData data() {
                return new RegressorData() {
                    @Override
                    public TrainingMethod trainerMethod() {
                        return TrainingMethod.LinearRegression;
                    }

                    @Override
                    public int featureDimension() {
                        return 4;
                    }
                };
            }
        };

        var metric = RegressionMetrics.MEAN_ABSOLUTE_ERROR;

        var trainStats = new TrainingStatistics(List.of(metric));

        trainStats.addTestScore(metric, 0.799999);
        trainStats.addOuterTrainScore(metric, 0.666666);
        trainStats.addCandidateStats(ModelCandidateStats.of(modelCandidate,
            Map.of(metric, ModelStats.of(0.89999, 0.79999, 0.99999)),
            Map.of(metric, ModelStats.of(0.649999, 0.499999, 0.7999999))
        ));

        var trainResult = ImmutableNodeRegressionTrainResult.of(regressor, trainStats);

        var converter = new NodeRegressionToModelConverter(pipeline, config);

        var result = converter.toModel(trainResult, graphStore.schema());

        var model = result.model();

        assertThat(model.algoType()).isEqualTo(NodeRegressionTrainingPipeline.MODEL_TYPE);
        assertThat(model.trainConfig()).isEqualTo(config);
        assertThat(model.data()).isInstanceOf(Regressor.RegressorData.class);
        assertThat(model.creator()).isEqualTo(config.username());
        assertThat(model.graphSchema()).isEqualTo(graphStore.schema());
        assertThat(model.name()).isEqualTo(config.modelName());
        assertThat(model.stored()).isFalse();
        assertThat(model.isPublished()).isFalse();

        assertThat(model.customInfo().bestCandidate().trainerConfig().toMap()).isEqualTo(modelCandidate.toMap());
        assertThat(model.customInfo().outerTrainMetrics().keySet()).containsExactly(evaluationMetric);
        assertThat(model.customInfo().testMetrics().keySet()).containsExactly(evaluationMetric);
        assertThat(model.customInfo().bestCandidate().trainingStats().keySet()).containsExactly(evaluationMetric);
        assertThat(model.customInfo().bestCandidate().validationStats().keySet()).containsExactly(evaluationMetric);
    }

}
