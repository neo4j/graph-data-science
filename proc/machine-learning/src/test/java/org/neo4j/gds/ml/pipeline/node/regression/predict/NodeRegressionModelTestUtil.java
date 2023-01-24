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
package org.neo4j.gds.ml.pipeline.node.regression.predict;

import org.neo4j.gds.api.schema.MutableGraphSchema;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.metrics.ModelCandidateStats;
import org.neo4j.gds.ml.models.Regressor;
import org.neo4j.gds.ml.models.linearregression.ImmutableLinearRegressionData;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionTrainConfig;
import org.neo4j.gds.ml.models.linearregression.LinearRegressor;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionPipelineTrainConfigImpl;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainingPipeline;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

final class NodeRegressionModelTestUtil {

    private NodeRegressionModelTestUtil() {}


    static Model<Regressor.RegressorData, NodeRegressionPipelineTrainConfig, NodeRegressionPipelineModelInfo> createModel(
        String username,
        String modelName,
        Regressor.RegressorData data,
        Stream<String> featureProperties
    ) {
        return Model.of(
            NodeRegressionTrainingPipeline.MODEL_TYPE,
            MutableGraphSchema.empty(),
            data,
            NodeRegressionPipelineTrainConfigImpl.builder()
               .modelUser(username)
               .pipeline("DUMMY")
               .graphName("DUMMY")
               .modelName(modelName)
               .targetProperty("target")
               .metrics(List.of("MEAN_SQUARED_ERROR"))
               .build(),
            NodeRegressionPipelineModelInfo.builder()
               .pipeline(NodePropertyPredictPipeline.from(
                   Stream.of(),
                   featureProperties.map(NodeFeatureStep::new))
               )
               .bestCandidate(ModelCandidateStats.of(
                   LinearRegressionTrainConfig.DEFAULT,
                   Map.of(),
                   Map.of()
               ))
               .build()
        );
    }

    static LinearRegressor createModelData(double[] weights, double bias) {
        return new LinearRegressor(ImmutableLinearRegressionData.builder()
            .weights(new Weights<>(
                new Matrix(
                    weights,
                    1,
                    weights.length
                ))
            )
            .bias(new Weights<>(new Scalar(bias)))
            .build()
        );
    }
}
