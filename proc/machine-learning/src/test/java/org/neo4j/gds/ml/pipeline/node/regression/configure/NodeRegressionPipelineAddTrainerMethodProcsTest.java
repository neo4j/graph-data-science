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
package org.neo4j.gds.ml.pipeline.node.regression.configure;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionTrainConfigImpl;
import org.neo4j.gds.ml.models.randomforest.RandomForestRegressorTrainerConfigImpl;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainingPipeline;
import org.neo4j.gds.ml_api.TrainingMethod;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NodeRegressionPipelineAddTrainerMethodProcsTest extends NodeRegressionPipelineBaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(NodeRegressionPipelineAddTrainerMethodProcs.class);
    }

    @Test
    void addLinearRegression() {
        var pipeline = new NodeRegressionTrainingPipeline();
        PipelineCatalog.set(getUsername(), "myPipe", pipeline);

        assertThat(pipeline.trainingParameterSpace().get(TrainingMethod.LinearRegression)).isEmpty();

        var expectedTrainConfig = LinearRegressionTrainConfigImpl
            .builder()
            .maxEpochs(5)
            .build();

        assertCypherResult(
            "CALL gds.alpha.pipeline.nodeRegression.addLinearRegression('myPipe', {maxEpochs: 5}) YIELD parameterSpace",
            List.of(Map.of(
                "parameterSpace", Map.of(
                    TrainingMethod.RandomForestRegression.toString(), List.of(),
                    TrainingMethod.LinearRegression.toString(), List.of(expectedTrainConfig.toMapWithTrainerMethod())
                )))
        );

        assertThat(pipeline.trainingParameterSpace().get(TrainingMethod.LinearRegression)).hasSize(1);

        runQuery("CALL gds.alpha.pipeline.nodeRegression.addLinearRegression('myPipe', {tolerance: {range: [1e-5, 1e-3]}})");

        assertThat(pipeline.trainingParameterSpace().get(TrainingMethod.LinearRegression)).hasSize(2);
    }

    @Test
    void addRandomForest() {
        var pipeline = new NodeRegressionTrainingPipeline();
        PipelineCatalog.set(getUsername(), "myPipe", pipeline);

        assertThat(pipeline.trainingParameterSpace().get(TrainingMethod.RandomForestRegression)).isEmpty();

        var expectedTrainConfig = RandomForestRegressorTrainerConfigImpl
            .builder()
            .numberOfDecisionTrees(5)
            .build();

        assertCypherResult(
            "CALL gds.alpha.pipeline.nodeRegression.addRandomForest('myPipe', {numberOfDecisionTrees: 5}) YIELD parameterSpace",
            List.of(Map.of(
                "parameterSpace", Map.of(
                    TrainingMethod.RandomForestRegression.toString(), List.of(expectedTrainConfig.toMapWithTrainerMethod()),
                    TrainingMethod.LinearRegression.toString(), List.of()
                )))
            );

        assertThat(pipeline.trainingParameterSpace().get(TrainingMethod.RandomForestRegression)).hasSize(1);

        runQuery("CALL gds.alpha.pipeline.nodeRegression.addRandomForest('myPipe', {numberOfDecisionTrees: {range: [10, 100]}})");

        assertThat(pipeline.trainingParameterSpace().get(TrainingMethod.RandomForestRegression)).hasSize(2);
    }

    @Test
    void addMixed() {
        var pipeline = new NodeRegressionTrainingPipeline();
        PipelineCatalog.set(getUsername(), "myPipe", pipeline);

        assertThat(pipeline.trainingParameterSpace().get(TrainingMethod.RandomForestRegression)).isEmpty();

        runQuery("CALL gds.alpha.pipeline.nodeRegression.addRandomForest('myPipe', {numberOfDecisionTrees: 5})");
        runQuery("CALL gds.alpha.pipeline.nodeRegression.addRandomForest('myPipe', {numberOfDecisionTrees: {range: [10, 100]}})");
        runQuery("CALL gds.alpha.pipeline.nodeRegression.addLinearRegression('myPipe', {maxEpochs: 42})");

        assertThat(pipeline.trainingParameterSpace().get(TrainingMethod.LinearRegression)).hasSize(1);
        assertThat(pipeline.trainingParameterSpace().get(TrainingMethod.RandomForestRegression)).hasSize(2);
    }
}
