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
package org.neo4j.gds.core.model;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import org.neo4j.gds.api.schema.SchemaDeserializer;
import org.neo4j.gds.api.schema.SchemaSerializer;
import org.neo4j.gds.model.storage.ModelInfoSerializerFactory;
import org.neo4j.gds.model.storage.TrainConfigSerializerFactory;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.ModelConfig;
import org.neo4j.graphalgo.core.model.proto.ModelProto;

import java.io.IOException;

public final class ModelMetaDataSerializer {

    private ModelMetaDataSerializer() {}

    public static ModelProto.ModelMetaData toSerializable(Model<?, ?> model) throws IOException {
        var builder = ModelProto.ModelMetaData.newBuilder();
        serializableTrainConfig(model, builder);
        serializeCustomInfo(model, builder);
        return builder
            .setCreator(model.creator())
            .addAllSharedWith(model.sharedWith())
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
            .creator(protoModelMetaData.getCreator())
            .sharedWith(protoModelMetaData.getSharedWithList())
            .name(protoModelMetaData.getName())
            .algoType(protoModelMetaData.getAlgoType())
            .graphSchema(SchemaDeserializer.graphSchema(protoModelMetaData.getGraphSchema()))
            .trainConfig(trainConfig(protoModelMetaData))
            .customInfo(deserializeCustomInfo(protoModelMetaData))
            .stored(true)
            .creationTime(ZonedDateTimeSerializer.fromSerializable(protoModelMetaData.getCreationTime()));
    }

    public static ModelProto.ModelMetaData toPublishable(
        ModelProto.ModelMetaData fromMetaData,
        String publicModelName,
        Iterable<String> sharedWith
    ) {
        return ModelProto.ModelMetaData.newBuilder(fromMetaData)
            .setName(publicModelName)
            .addAllSharedWith(sharedWith)
            .build();
    }

    private static void serializableTrainConfig(Model<?, ?> model, ModelProto.ModelMetaData.Builder builder) {
        var trainConfigSerializer =
            TrainConfigSerializerFactory.trainConfigSerializer(model.algoType());
        var serializable = trainConfigSerializer.toSerializable(model.trainConfig());
        builder.setTrainConfig(Any.pack(serializable));
    }

    private static <CONFIG extends ModelConfig & BaseConfig> CONFIG trainConfig(
        ModelProto.ModelMetaData protoModelMetaData
    ) {
        var modelConfigSerializer =
            TrainConfigSerializerFactory.trainConfigSerializer(protoModelMetaData.getAlgoType());
        return (CONFIG) modelConfigSerializer.fromSerializable(protoModelMetaData.getTrainConfig());
    }

    private static void serializeCustomInfo(Model<?, ?> model, ModelProto.ModelMetaData.Builder builder) {
        var serializable = ModelInfoSerializerFactory
            .modelInfoSerializer(model.algoType())
            .toSerializable(model.customInfo());
        builder.setCustomInfo(Any.pack(serializable));
    }

    private static Model.Mappable deserializeCustomInfo(ModelProto.ModelMetaData protoModelMetaData) {
        try {
            var serializable = ModelInfoSerializerFactory
                .modelInfoSerializer(protoModelMetaData.getAlgoType());
            var customInfo = protoModelMetaData.getCustomInfo();
            var unpacked = customInfo.unpack(serializable.serializableClass());
            return serializable.fromSerializable(unpacked);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
