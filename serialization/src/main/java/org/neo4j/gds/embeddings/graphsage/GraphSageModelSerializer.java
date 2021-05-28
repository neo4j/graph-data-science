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

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Parser;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.ModelSerializer;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelMetaDataSerializer;
import org.neo4j.graphalgo.core.model.proto.GraphSageProto;
import org.neo4j.graphalgo.core.model.proto.ModelProto;

import java.io.IOException;

public final class GraphSageModelSerializer implements ModelSerializer {

    @Override
    public GeneratedMessageV3 toSerializable(Object data) throws IOException {
        var modelData = (ModelData) data;
        var modelDataBuilder = GraphSageProto.ModelData.newBuilder();
        for (int i = 0; i < modelData.layers().length; i++) {
            GraphSageProto.Layer layer = LayerSerializer.toSerializable(modelData.layers()[i]);
            modelDataBuilder.addLayers(i, layer);
        }

        return GraphSageProto.GraphSageModel.newBuilder()
            .setData(modelDataBuilder)
            .setFeatureFunction(FeatureFunctionSerializer.toSerializable(modelData.featureFunction()))
            .build();
    }

    @Override
    @NotNull
    public ModelData deserializeModelData(GeneratedMessageV3 generatedMessageV3) throws IOException {
        var protoModel = (GraphSageProto.GraphSageModel) generatedMessageV3;
        return ModelData.of(
            LayerSerializer.fromSerializable(protoModel.getData().getLayersList()),
            FeatureFunctionSerializer.fromSerializable(protoModel.getFeatureFunction())
        );
    }

    @Override
    public Parser<GraphSageProto.GraphSageModel> modelParser() {
        return GraphSageProto.GraphSageModel.parser();
    }

    @TestOnly
    Model<ModelData, GraphSageTrainConfig> fromSerializable(
        GeneratedMessageV3 protoModel,
        ModelProto.ModelMetaData modelMetaData
    ) throws IOException {
        return ModelMetaDataSerializer
            .<ModelData, GraphSageTrainConfig>fromSerializable(modelMetaData)
            .data(deserializeModelData(protoModel))
            .build();
    }
}
