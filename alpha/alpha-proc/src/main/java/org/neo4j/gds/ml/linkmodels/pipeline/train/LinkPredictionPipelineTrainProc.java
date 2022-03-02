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

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.TrainProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.ml.MLTrainResult;
import org.neo4j.gds.ml.PipelineCompanion;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrain;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.TRAIN;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.beta.pipeline.linkPrediction.train", description = "Trains a link prediction model based on a pipeline", executionMode = TRAIN)
public class LinkPredictionPipelineTrainProc extends TrainProc<
    LinkPredictionTrainPipelineExecutor,
    LinkPredictionTrainResult,
    LinkPredictionTrainConfig,
    LinkPredictionPipelineTrainProc.LPTrainResult
    > {

    @Procedure(name = "gds.beta.pipeline.linkPrediction.train", mode = Mode.READ)
    @Description("Trains a link prediction model based on a pipeline")
    public Stream<LPTrainResult> train(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> config
    ) {
        PipelineCompanion.prepareTrainConfig(graphName, config);
        return trainAndStoreModelWithResult(compute(graphName, config));
    }

    @Procedure(name = "gds.beta.pipeline.linkPrediction.train.estimate", mode = READ)
    @Description("Estimates memory for applying a linkPrediction model")
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        PipelineCompanion.prepareTrainConfig(graphNameOrConfiguration, algoConfiguration);
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected LinkPredictionTrainConfig newConfig(String username, CypherMapWrapper config) {
        return LinkPredictionTrainConfig.of(username(), config);
    }

    @Override
    public GraphStoreAlgorithmFactory<LinkPredictionTrainPipelineExecutor, LinkPredictionTrainConfig> algorithmFactory() {
        return new LinkPredictionTrainPipelineAlgorithmFactory(executionContext());
    }

    @Override
    protected String modelType() {
        return LinkPredictionTrain.MODEL_TYPE;
    }

    @Override
    protected Model<?, ?, ?> extractModel(LinkPredictionTrainResult algoResult) {
        return algoResult.model();
    }

    @Override
    protected LPTrainResult constructProcResult(
        ComputationResult<LinkPredictionTrainPipelineExecutor, LinkPredictionTrainResult, LinkPredictionTrainConfig> computationResult
    ) {
        return new LPTrainResult(computationResult.result(), computationResult.computeMillis());
    }

    public static class LPTrainResult extends MLTrainResult {

        public final Map<String, Object> modelSelectionStats;

        public LPTrainResult(LinkPredictionTrainResult algoResult, long trainMillis) {
            super(algoResult.model(), trainMillis);

            this.modelSelectionStats = algoResult.modelSelectionStatistics().toMap();
        }
    }
}
