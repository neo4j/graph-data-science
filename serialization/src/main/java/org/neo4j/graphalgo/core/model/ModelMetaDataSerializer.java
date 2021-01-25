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
package org.neo4j.graphalgo.core.model;

import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.graphalgo.api.schema.SchemaDeserializer;
import org.neo4j.graphalgo.api.schema.SchemaSerializer;
import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.GraphSageTrainConfigSerializer;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.core.model.proto.ModelProto;

import java.io.IOException;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class ModelMetaDataSerializer {

    private ModelMetaDataSerializer() {}

    public static ModelProto.ModelMetaData toSerializable(Model<?, ?> model) throws IOException {
        var builder = ModelProto.ModelMetaData.newBuilder();
        serializableTrainConfig(model, builder);
        return builder
            .setUsername(model.username())
            .setName(model.name())
            .setAlgoType(model.algoType())
            .setGraphSchema(SchemaSerializer.serializableGraphSchema(model.graphSchema()))
            .setCreationTime(ZonedDateTimeSerializer.toSerializable(model.creationTime()))
            .build();
    }

    public static <DATA, CONFIG extends ModelConfig & BaseConfig> ImmutableModel.Builder<DATA, CONFIG> fromSerializable(
        ModelProto.ModelMetaData protoModelMetaData
    ) {
        return ImmutableModel.<DATA, CONFIG>builder()
            .username(protoModelMetaData.getUsername())
            .name(protoModelMetaData.getName())
            .algoType(protoModelMetaData.getAlgoType())
            .graphSchema(SchemaDeserializer.graphSchema(protoModelMetaData.getGraphSchema()))
            .trainConfig(trainConfig(protoModelMetaData))
            .creationTime(ZonedDateTimeSerializer.fromSerializable(protoModelMetaData.getCreationTime()));
    }

    private static void serializableTrainConfig(Model<?, ?> model, ModelProto.ModelMetaData.Builder builder) {
        switch(model.algoType()) {
            case GraphSage.MODEL_TYPE:
                builder.setGraphSageTrainConfig(GraphSageTrainConfigSerializer.toSerializable((GraphSageTrainConfig) model
                    .trainConfig()));
                break;
            default:
                throw new RuntimeException(formatWithLocale("Unsupported model type: %s", model.algoType()));
        }
    }

    private static <CONFIG extends ModelConfig & BaseConfig>  CONFIG trainConfig(ModelProto.ModelMetaData protoModelMetaData) {
        switch(protoModelMetaData.getAlgoType()) {
            case GraphSage.MODEL_TYPE:
                return (CONFIG) GraphSageTrainConfigSerializer.fromSerializable(protoModelMetaData.getGraphSageTrainConfig());
            default:
                throw new RuntimeException(formatWithLocale("Unsupported model type: %s", protoModelMetaData.getAlgoType()));
        }
    }
}
