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

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Parser;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.ModelSerializer;
import org.neo4j.gds.core.model.ModelMetaDataSerializer;
import org.neo4j.gds.embeddings.ddl4j.tensor.TensorSerializer;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.core.model.Model;
import org.neo4j.graphalgo.core.model.proto.ModelProto;
import org.neo4j.graphalgo.ml.model.proto.NodeClassificationProto;

public class NodeClassificationSerializer implements ModelSerializer {

    @Override
    public GeneratedMessageV3 toSerializable(Object data) {
        var modelData = (NodeLogisticRegressionData) data;
        var weightsMatrix = modelData.weights().data();
        var serializableWeightsMatrix = TensorSerializer.toSerializable(weightsMatrix);
        return NodeClassificationProto.NodeClassificationModelData.newBuilder()
            .setWeights(serializableWeightsMatrix)
            .setLocalIdMap(NodeClassificationProto.LocalIdMap
                .newBuilder()
                .addAllOriginalIds(modelData.classIdMap().originalIdsList()))
            .build();
    }

    @Override
    @NotNull
    public NodeLogisticRegressionData deserializeModelData(GeneratedMessageV3 generatedMessageV3) {
        var serializedData = (NodeClassificationProto.NodeClassificationModelData) generatedMessageV3;
        var weights = new Weights<>(TensorSerializer.fromSerializable(serializedData.getWeights()));

        var localIdMap = new LocalIdMap();
        serializedData.getLocalIdMap()
            .getOriginalIdsList()
            .forEach(localIdMap::toMapped);

        return NodeLogisticRegressionData.builder()
            .weights(weights)
            .classIdMap(localIdMap)
            .build();
    }

    @Override
    public Parser<NodeClassificationProto.NodeClassificationModelData> modelParser() {
        return NodeClassificationProto.NodeClassificationModelData.parser();
    }

    @TestOnly
    Model<NodeLogisticRegressionData, NodeClassificationTrainConfig> fromSerializable(
        GeneratedMessageV3 generatedMessageV3,
        ModelProto.ModelMetaData modelMetaData
    ) {
        var serializedData = (NodeClassificationProto.NodeClassificationModelData) generatedMessageV3;
        var weights = new Weights<>(TensorSerializer.fromSerializable(serializedData.getWeights()));

        var localIdMap = new LocalIdMap();
        serializedData.getLocalIdMap()
            .getOriginalIdsList()
            .forEach(localIdMap::toMapped);

        return ModelMetaDataSerializer
            .<NodeLogisticRegressionData, NodeClassificationTrainConfig>fromSerializable(modelMetaData)
            .data(NodeLogisticRegressionData.builder()
                .weights(weights)
                .classIdMap(localIdMap)
                .build())
            .build();
    }
}
