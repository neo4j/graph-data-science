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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.TrainProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.ml.MLTrainResult;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class LinkPredictionPipelineTrainProc extends TrainProc<LinkPredictionTrain, LinkLogisticRegressionData, LinkPredictionTrainConfig> {

    @Procedure(name = "gds.alpha.ml.pipeline.linkPrediction.train", mode = Mode.READ)
    @Description("Trains a link prediction model")
    public Stream<MLTrainResult> train(@Name(value = "graphName") String graphName, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> config
    ) {
        return trainAndStoreModelWithResult(graphName, config, (model, result) -> new MLTrainResult(model, result.computeMillis()));
    }

    @Override
    protected LinkPredictionTrainConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LinkPredictionTrainConfig.of(username(), graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<LinkPredictionTrain, LinkPredictionTrainConfig> algorithmFactory() {
        return new AlgorithmFactory<>() {
            @Override
            public LinkPredictionTrain build(
                Graph graph,
                LinkPredictionTrainConfig trainConfig,
                AllocationTracker tracker,
                Log log,
                ProgressEventTracker eventTracker
            ) {
                String graphName = trainConfig
                    .graphName()
                    .orElseThrow(() -> new UnsupportedOperationException(
                        "Pipelines cannot be used with anonymous graphs. Please load the graph before"));
                var graphStore = GraphStoreCatalog.get(username(), databaseId(), graphName).graphStore();

                var pipeline = PipelineUtils.getPipelineModelInfo(trainConfig.pipeline(), username());

                pipeline.validate();

                return new LinkPredictionTrain(
                    graphStore,
                    trainConfig,
                    pipeline,
                    new PipelineExecutor(
                        pipeline,
                        LinkPredictionPipelineTrainProc.this,
                        databaseId(),
                        username(),
                        graphName
                    ),
                    ProgressTracker.EmptyProgressTracker.NULL_TRACKER
                );
            }

            @Override
            public MemoryEstimation memoryEstimation(LinkPredictionTrainConfig configuration) {
                throw new MemoryEstimationNotImplementedException();
            }
        };
    }

    @Override
    protected String modelType() {
        return LinkPredictionTrain.MODEL_TYPE;
    }
}
