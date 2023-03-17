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
import org.neo4j.gds.compat.ProxyUtil;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.ml.MLTrainResult;
import org.neo4j.gds.ml.pipeline.PipelineCompanion;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainPipelineExecutor;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.TRAIN;
import static org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainPipelineExecutor.LinkPredictionTrainPipelineResult;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.beta.pipeline.linkPrediction.train", description = "Trains a link prediction model based on a pipeline", executionMode = TRAIN)
public class LinkPredictionPipelineTrainProc extends TrainProc<
    LinkPredictionTrainPipelineExecutor,
    LinkPredictionTrainPipelineResult,
    LinkPredictionTrainConfig,
    LinkPredictionPipelineTrainProc.LPTrainResult
    > {

    @Procedure(name = "gds.beta.pipeline.linkPrediction.train", mode = Mode.READ)
    @Description("Trains a link prediction model based on a pipeline")
    public Stream<LPTrainResult> train(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> config
    ) {
        PipelineCompanion.preparePipelineConfig(graphName, config);
        return trainAndStoreModelWithResult(compute(graphName, config));
    }

    @Procedure(name = "gds.beta.pipeline.linkPrediction.train.estimate", mode = READ)
    @Description("Estimates memory for applying a linkPrediction model")
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameOrConfiguration, algoConfiguration);
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected LinkPredictionTrainConfig newConfig(String username, CypherMapWrapper config) {
        return LinkPredictionTrainConfig.of(username(), config);
    }

    @Override
    public GraphStoreAlgorithmFactory<LinkPredictionTrainPipelineExecutor, LinkPredictionTrainConfig> algorithmFactory() {
        var gdsVersion = ProxyUtil.GDS_VERSION_INFO.gdsVersion();
        return new LinkPredictionTrainPipelineAlgorithmFactory(executionContext(), gdsVersion);
    }

    @Override
    protected String modelType() {
        return LinkPredictionTrainingPipeline.MODEL_TYPE;
    }

    @Override
    protected Model<?, ?, ?> extractModel(LinkPredictionTrainPipelineResult algoResult) {
        return algoResult.model();
    }

    @Override
    protected LPTrainResult constructProcResult(
        ComputationResult<LinkPredictionTrainPipelineExecutor, LinkPredictionTrainPipelineResult, LinkPredictionTrainConfig> computationResult
    ) {
        return new LPTrainResult(computationResult.result(), computationResult.computeMillis());
    }

    @SuppressWarnings("unused")
    public static class LPTrainResult extends MLTrainResult {

        public final Map<String, Object> modelSelectionStats;

        public LPTrainResult(LinkPredictionTrainPipelineResult algoResult, long trainMillis) {
            super(algoResult.model(), trainMillis);

            this.modelSelectionStats = algoResult.trainingStatistics().toMap();
        }
    }
}
