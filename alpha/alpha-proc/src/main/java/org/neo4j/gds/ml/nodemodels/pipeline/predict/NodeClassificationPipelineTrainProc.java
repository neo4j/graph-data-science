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
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.MLTrainResult;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrain;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainPipelineAlgorithmFactory;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationTrainPipelineExecutor;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

public class NodeClassificationPipelineTrainProc
    extends TrainProc<
        NodeClassificationTrainPipelineExecutor,
        NodeLogisticRegressionData,
        NodeClassificationPipelineTrainConfig,
        NodeClassificationPipelineModelInfo
    > {

    @Context
    public ModelCatalog modelCatalog;

    @Procedure(name = "gds.alpha.ml.pipeline.nodeClassification.train", mode = Mode.READ)
    @Description("Trains a node classification model based on a pipeline")
    public Stream<MLTrainResult> train(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        // TODO: this will go away once node property steps do not rely on this method
        configuration.put("graphName", graphName);
        return trainAndStoreModelWithResult(graphName, configuration, (model, result) -> new MLTrainResult(model, result.computeMillis()));
    }

    @Override
    protected NodeClassificationPipelineTrainConfig newConfig(String username, CypherMapWrapper config) {
        return NodeClassificationPipelineTrainConfig.of(username, config);
    }

    @Override
    protected GraphStoreAlgorithmFactory<NodeClassificationTrainPipelineExecutor, NodeClassificationPipelineTrainConfig> algorithmFactory() {
        return new NodeClassificationTrainPipelineAlgorithmFactory(this, modelCatalog);
    }

    @Override
    protected String modelType() {
        return NodeClassificationTrain.MODEL_TYPE;
    }
}
