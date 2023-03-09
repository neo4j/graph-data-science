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

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.core.ConfigKeyValidation;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionTrainConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestRegressorTrainerConfig;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.node.NodePipelineInfoResult;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainingPipeline;
import org.neo4j.gds.ml_api.TrainingMethod;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class NodeRegressionPipelineAddTrainerMethodProcs extends BaseProc {

    @Procedure(name = "gds.alpha.pipeline.nodeRegression.addLinearRegression", mode = READ)
    @Description("Add a linear regression model candidate to a node regression pipeline.")
    public Stream<NodePipelineInfoResult> addLogisticRegression(
        @Name("pipelineName") String pipelineName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var pipeline = PipelineCatalog.getTyped(username(), pipelineName, NodeRegressionTrainingPipeline.class);

        var allowedKeys = LinearRegressionTrainConfig.DEFAULT.configKeys();
        ConfigKeyValidation.requireOnlyKeysFrom(allowedKeys, configuration.keySet());

        pipeline.addTrainerConfig(TunableTrainerConfig.of(configuration, TrainingMethod.LinearRegression));

        return Stream.of(new NodePipelineInfoResult(pipelineName, pipeline));
    }

    @Procedure(name = "gds.alpha.pipeline.nodeRegression.addRandomForest", mode = READ)
    @Description("Add a random forest model candidate to a node regression pipeline.")
    public Stream<NodePipelineInfoResult> addRandomForest(
        @Name("pipelineName") String pipelineName,
        @Name(value = "configuration") Map<String, Object> configuration
    ) {
        var pipeline = PipelineCatalog.getTyped(username(), pipelineName, NodeRegressionTrainingPipeline.class);

        var allowedKeys = RandomForestRegressorTrainerConfig.DEFAULT.configKeys();
        ConfigKeyValidation.requireOnlyKeysFrom(allowedKeys, configuration.keySet());

        pipeline.addTrainerConfig(TunableTrainerConfig.of(configuration, TrainingMethod.RandomForestRegression));

        return Stream.of(new NodePipelineInfoResult(pipelineName, pipeline));
    }
}
