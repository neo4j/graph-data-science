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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.TrainProc;
import org.neo4j.gds.compat.ProxyUtil;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrain;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.embeddings.graphsage.GraphSageCompanion.GRAPHSAGE_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.TRAIN;

@GdsCallable(name = "gds.beta.graphSage.train", description = GRAPHSAGE_DESCRIPTION, executionMode = TRAIN)
public class GraphSageTrainProc extends TrainProc<GraphSageTrain, Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics>, GraphSageTrainConfig, TrainProc.TrainResult> {

    @Description(GRAPHSAGE_DESCRIPTION)
    @Procedure(name = "gds.beta.graphSage.train", mode = Mode.READ)
    public Stream<TrainResult> train(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return trainAndStoreModelWithResult(compute(graphName, configuration));
    }

    @Description(ESTIMATE_DESCRIPTION)
    @Procedure(name = "gds.beta.graphSage.train.estimate", mode = Mode.READ)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected GraphSageTrainConfig newConfig(String username, CypherMapWrapper config) {
        return GraphSageTrainConfig.of(username, config);
    }

    @Override
    public GraphAlgorithmFactory<GraphSageTrain, GraphSageTrainConfig> algorithmFactory() {
        var gdsVersion = ProxyUtil.GDS_VERSION_INFO.gdsVersion();
        return new GraphSageTrainAlgorithmFactory(gdsVersion);
    }

    @Override
    protected String modelType() {
        return GraphSage.MODEL_TYPE;
    }

    @Override
    protected TrainResult constructProcResult(ComputationResult<GraphSageTrain, Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics>, GraphSageTrainConfig> computationResult) {
        return new TrainResult(
            computationResult.result(),
            computationResult.computeMillis(),
            computationResult.graph().nodeCount(),
            computationResult.graph().relationshipCount()
        );
    }

    @Override
    protected Model<?, ?, ?> extractModel(Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> model) {
        return model;
    }
}

