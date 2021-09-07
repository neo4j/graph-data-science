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
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.MLTrainResult;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class LinkPredictionPipelineTrainProc extends TrainProc<LinkPredictionTrain, LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo> {

    @Procedure(name = "gds.alpha.ml.pipeline.linkPrediction.train", mode = Mode.READ)
    @Description("Trains a link prediction model based on a pipeline")
    public Stream<MLTrainResult> train(@Name(value = "graphName") Object graphNameOrConfig, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> config
    ) {
        return trainAndStoreModelWithResult(graphNameOrConfig, config, (model, result) -> new MLTrainResult(model, result.computeMillis()));
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
        return new LinkPredictionTrainFactory(databaseId(), this);
    }

    @Override
    protected String modelType() {
        return LinkPredictionTrain.MODEL_TYPE;
    }
}
