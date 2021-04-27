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
package org.neo4j.gds.ml.nodemodels;

import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.subgraph.LocalIdMap;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRData;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.ml.nodemodels.NodeClassificationTrain.MODEL_TYPE;

public final class NodeClassificationPredictProcTestUtil {

    private NodeClassificationPredictProcTestUtil() {}

    public static void addModelWithFeatures(String username, String modelName, List<String> properties) {
        var classIdMap = new LocalIdMap();
        classIdMap.toMapped(0);
        classIdMap.toMapped(1);
        var model = Model.of(
            username,
            modelName,
            MODEL_TYPE,
            GraphSchema.empty(),
            MultiClassNLRData.builder()
                .weights(new Weights<>(new Matrix(new double[]{
                    1.12730619, -0.84532386, 0.93216654,
                    -1.12730619, 0.84532386, 0.0
                }, 2, 3)))
                .classIdMap(classIdMap)
                .build(),
            ImmutableNodeClassificationTrainConfig
                .builder()
                .modelName(modelName)
                .targetProperty("foo")
                .holdoutFraction(0.25)
                .validationFolds(4)
                .featureProperties(properties)
                .addParam(Map.of("penalty", 1.0))
                .build()
        );
        ModelCatalog.set(model);
    }
}
