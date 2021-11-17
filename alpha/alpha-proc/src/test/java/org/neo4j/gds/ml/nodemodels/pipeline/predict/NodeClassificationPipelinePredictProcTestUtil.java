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

import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.nodemodels.logisticregression.ImmutableNodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationFeatureStep;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipeline;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineExecutor;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;

import java.util.List;
import java.util.Map;

public final class NodeClassificationPipelinePredictProcTestUtil {

    private NodeClassificationPipelinePredictProcTestUtil() {}

    public static void addPipelineModelWithFeatures(ModelCatalog modelCatalog, String username, int dimensionOfNodeFeatures) {
        addPipelineModelWithFeatures(modelCatalog, username, dimensionOfNodeFeatures, List.of("a","b"));
    }

    public static void addPipelineModelWithFeatures(ModelCatalog modelCatalog, String username, int dimensionOfNodeFeatures, List<String> nodeFeatures) {
        var pipeline = new NodeClassificationPipeline();
        pipeline.addNodePropertyStep(NodePropertyStep.of("degree", Map.of("mutateProperty", "degree")));
        for (String nodeFeature : nodeFeatures)
            pipeline.addFeatureStep(NodeClassificationFeatureStep.of(nodeFeature));
        pipeline.addFeatureStep(NodeClassificationFeatureStep.of("degree"));
        var weights=new double[2*(dimensionOfNodeFeatures+2)];
        for (int i=0; i<weights.length;++i)
            weights[i]=i;
        NodeLogisticRegressionData modelData = createModeldata(weights);
        modelCatalog.set(Model.of(
            username,
            "model",
            NodeClassificationPipelineExecutor.MODEL_TYPE,
            GraphSchema.empty(),
            modelData,
            NodeClassificationPipelineTrainConfig.builder()
                .modelName("model")
                .pipeline("DUMMY")
                .targetProperty("foo")
                .build(),
            NodeClassificationPipelineModelInfo.builder()
                .classes(modelData.classIdMap().originalIdsList())
                .bestParameters(NodeLogisticRegressionTrainConfig.of(List.of("foo","bar"),"foo",Map.of()))
                .metrics(Map.of())
                .trainingPipeline(pipeline.copy())
                .build()
        ));
    }

    static NodeLogisticRegressionData createModeldata(double[] weights) {
        var idMap = new LocalIdMap();
        idMap.toMapped(0);
        idMap.toMapped(1);
        return ImmutableNodeLogisticRegressionData.of(
            new Weights<>(
                new Matrix(
                    weights,
                    2,
                    weights.length/2
                )),
            idMap
        );
    }
}
