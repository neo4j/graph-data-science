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
package org.neo4j.gds.ml.nodemodels.pipeline.predict;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.TrainProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.ml.MLTrainResult;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrain;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainPipelineAlgorithmFactory;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineTrainResult;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationTrainPipelineExecutor;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.TRAIN;
import static org.neo4j.gds.ml.PipelineCompanion.prepareTrainConfig;

@GdsCallable(name = "gds.beta.pipeline.nodeClassification.train", description = "Trains a node classification model based on a pipeline", executionMode = TRAIN)
public class NodeClassificationPipelineTrainProc extends TrainProc<
    NodeClassificationTrainPipelineExecutor,
    NodeClassificationPipelineTrainResult,
    NodeClassificationPipelineTrainConfig,
    NodeClassificationPipelineTrainProc.NCTrainResult
    > {

    @Procedure(name = "gds.beta.pipeline.nodeClassification.train", mode = Mode.READ)
    @Description("Trains a node classification model based on a pipeline")
    public Stream<NCTrainResult> train(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        prepareTrainConfig(graphName, configuration);
        return trainAndStoreModelWithResult(compute(graphName, configuration));
    }

    @Procedure(name = "gds.beta.pipeline.nodeClassification.train.estimate", mode = Mode.READ)
    @Description("Estimates memory for training a node classification model based on a pipeline")
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        prepareTrainConfig(graphNameOrConfiguration, algoConfiguration);
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected NodeClassificationPipelineTrainConfig newConfig(String username, CypherMapWrapper config) {
        return NodeClassificationPipelineTrainConfig.of(username, config);
    }

    @Override
    public GraphStoreAlgorithmFactory<NodeClassificationTrainPipelineExecutor, NodeClassificationPipelineTrainConfig> algorithmFactory() {
        return new NodeClassificationTrainPipelineAlgorithmFactory(executionContext());
    }

    @Override
    protected String modelType() {
        return NodeClassificationTrain.MODEL_TYPE;
    }

    @Override
    protected NCTrainResult constructProcResult(ComputationResult<NodeClassificationTrainPipelineExecutor, NodeClassificationPipelineTrainResult, NodeClassificationPipelineTrainConfig> computationResult) {
        return new NCTrainResult(computationResult.result(), computationResult.computeMillis());
    }

    @Override
    protected Model<?, ?, ?> extractModel(NodeClassificationPipelineTrainResult algoResult) {
        return algoResult.model();
    }

    public static class NCTrainResult extends MLTrainResult {

        public final Map<String, Object> modelSelectionStats;

        public NCTrainResult(NodeClassificationPipelineTrainResult algoResult, long trainMillis) {
            super(algoResult.model(), trainMillis);
            this.modelSelectionStats = algoResult.modelSelectionStatistics().toMap();
        }
    }
}
