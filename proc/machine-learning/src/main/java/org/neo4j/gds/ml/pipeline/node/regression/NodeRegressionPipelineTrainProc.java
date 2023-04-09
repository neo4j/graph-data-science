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
package org.neo4j.gds.ml.pipeline.node.regression;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.TrainProc;
import org.neo4j.gds.compat.ProxyUtil;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.ml.MLTrainResult;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainAlgorithm;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainPipelineAlgorithmFactory;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainingPipeline;
import org.neo4j.gds.ml.training.TrainingStatistics;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.TRAIN;
import static org.neo4j.gds.ml.pipeline.PipelineCompanion.preparePipelineConfig;
import static org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainResult.NodeRegressionTrainPipelineResult;

@GdsCallable(name = "gds.alpha.pipeline.nodeRegression.train", description = "Trains a node regression model based on a pipeline", executionMode = TRAIN)
public class NodeRegressionPipelineTrainProc extends TrainProc<
    NodeRegressionTrainAlgorithm,
    NodeRegressionTrainPipelineResult,
    NodeRegressionPipelineTrainConfig,
    NodeRegressionPipelineTrainProc.NRTrainResult
    > {

    @Procedure(name = "gds.alpha.pipeline.nodeRegression.train", mode = Mode.READ)
    @Description("Trains a node classification model based on a pipeline")
    public Stream<NRTrainResult> train(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        preparePipelineConfig(graphName, configuration);
        return trainAndSetModelWithResult(compute(graphName, configuration));
    }

    @Override
    protected NodeRegressionPipelineTrainConfig newConfig(String username, CypherMapWrapper config) {
        return NodeRegressionPipelineTrainConfig.of(username, config);
    }

    @Override
    public GraphStoreAlgorithmFactory<NodeRegressionTrainAlgorithm, NodeRegressionPipelineTrainConfig> algorithmFactory() {
        var gdsVersion = ProxyUtil.GDS_VERSION_INFO.gdsVersion();
        return new NodeRegressionTrainPipelineAlgorithmFactory(executionContext(), gdsVersion);
    }

    @Override
    protected String modelType() {
        return NodeRegressionTrainingPipeline.MODEL_TYPE;
    }

    @Override
    protected NRTrainResult constructProcResult(ComputationResult<NodeRegressionTrainAlgorithm, NodeRegressionTrainPipelineResult, NodeRegressionPipelineTrainConfig> computationResult) {
        return new NRTrainResult(computationResult.result(), computationResult.computeMillis());
    }

    @Override
    protected Model<?, ?, ?> extractModel(NodeRegressionTrainPipelineResult algoResult) {
        return algoResult.model();
    }

    public static class NRTrainResult extends MLTrainResult {

        public final Map<String, Object> modelSelectionStats;

        public NRTrainResult(Optional<NodeRegressionTrainPipelineResult> pipelineResult, long trainMillis) {
            super(pipelineResult.map(NodeRegressionTrainPipelineResult::model), trainMillis);
            this.modelSelectionStats = pipelineResult
                .map(NodeRegressionTrainPipelineResult::trainingStatistics)
                .map(TrainingStatistics::toMap)
                .orElseGet(Collections::emptyMap);
        }
    }
}
