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
package org.neo4j.gds.ml.pipeline.node.classification.predict;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.TrainProc;
import org.neo4j.gds.compat.ProxyUtil;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.ml.MLTrainResult;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationTrainAlgorithm;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationTrainPipelineAlgorithmFactory;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationTrainResult.NodeClassificationModelResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.TRAIN;
import static org.neo4j.gds.ml.pipeline.PipelineCompanion.preparePipelineConfig;

@GdsCallable(name = "gds.beta.pipeline.nodeClassification.train", description = "Trains a node classification model based on a pipeline", executionMode = TRAIN)
public class NodeClassificationPipelineTrainProc extends TrainProc<
    NodeClassificationTrainAlgorithm,
    NodeClassificationModelResult,
    NodeClassificationPipelineTrainConfig,
    NodeClassificationPipelineTrainProc.NCTrainResult
    > {

    @Procedure(name = "gds.beta.pipeline.nodeClassification.train", mode = Mode.READ)
    @Description("Trains a node classification model based on a pipeline")
    public Stream<NCTrainResult> train(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        preparePipelineConfig(graphName, configuration);
        return trainAndSetModelWithResult(compute(graphName, configuration));
    }

    @Procedure(name = "gds.beta.pipeline.nodeClassification.train.estimate", mode = Mode.READ)
    @Description("Estimates memory for training a node classification model based on a pipeline")
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        preparePipelineConfig(graphNameOrConfiguration, algoConfiguration);
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected NodeClassificationPipelineTrainConfig newConfig(String username, CypherMapWrapper config) {
        return NodeClassificationPipelineTrainConfig.of(username, config);
    }


    @Override
    public GraphStoreAlgorithmFactory<
        NodeClassificationTrainAlgorithm,
        NodeClassificationPipelineTrainConfig
    > algorithmFactory() {
        var gdsVersion = ProxyUtil.GDS_VERSION_INFO.gdsVersion();
        return new NodeClassificationTrainPipelineAlgorithmFactory(executionContext(), gdsVersion);
    }

    @Override
    protected String modelType() {
        return NodeClassificationTrainingPipeline.MODEL_TYPE;
    }

    @Override
    protected NCTrainResult constructProcResult(ComputationResult<
        NodeClassificationTrainAlgorithm,
        NodeClassificationModelResult,
        NodeClassificationPipelineTrainConfig> computationResult) {
        var transformedResult = computationResult.result();
        return new NCTrainResult(transformedResult, computationResult.computeMillis());
    }

    @Override
    protected Model<?, ?, ?> extractModel(NodeClassificationModelResult nodeClassificationModelResult) {
        return nodeClassificationModelResult.model();
    }

    public static class NCTrainResult extends MLTrainResult {

        public final Map<String, Object> modelSelectionStats;

        public NCTrainResult(NodeClassificationModelResult pipelineResult, long trainMillis) {
            super(pipelineResult.model(), trainMillis);
            this.modelSelectionStats = pipelineResult.trainingStatistics().toMap();
        }
    }
}
