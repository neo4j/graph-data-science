/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.model.storage;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Parser;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelSerializer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelMetaDataSerializer;
import org.neo4j.graphalgo.core.model.proto.GraphSageProto;
import org.neo4j.graphalgo.core.model.proto.ModelProto;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class ModelToFileExporter {

    public static final String META_DATA_SUFFIX = "meta";
    public static final String MODEL_DATA_SUFFIX = "data";

    private ModelToFileExporter() {}

    public static <DATA, CONFIG extends BaseConfig & ModelConfig> void toFile(Path exportDir, Model<DATA, CONFIG> model) throws IOException {
        writeMetaData(exportDir, model.name(), ModelMetaDataSerializer.toSerializable(model));
        writeModelData(exportDir, model.name(), toSerializable(model.data(), model.algoType()));
    }

    public static <DATA, CONFIG extends BaseConfig & ModelConfig> Model<DATA, CONFIG> fromFile(Path exportDir, String modelName) throws
        IOException, ClassNotFoundException {
        var modelMetaData = readMetaData(exportDir, modelName);
        return fromSerializable(exportDir, modelName, modelMetaData);
    }

    private static <DATA> GeneratedMessageV3 toSerializable(DATA data, String algoType) throws IOException {
        switch (algoType) {
            case GraphSage.MODEL_TYPE:
                return GraphSageModelSerializer.toSerializable((ModelData) data);
            default:
                throw new IllegalArgumentException(formatWithLocale("Algo type %s was not found.", algoType));
        }
    }

    private static <DATA, CONFIG extends BaseConfig & ModelConfig> Model<DATA, CONFIG> fromSerializable(
        Path exportDir,
        String modelName,
        ModelProto.ModelMetaData modelMetaData
    ) throws IOException, ClassNotFoundException {
        switch (modelMetaData.getAlgoType()) {
            case GraphSage.MODEL_TYPE:
                var parser = GraphSageProto.GraphSageModel.parser();
                var graphSageModelProto = readModelData(exportDir, modelName, parser);
                return (Model<DATA, CONFIG>) GraphSageModelSerializer.fromSerializable(graphSageModelProto, modelMetaData);
            default:
                throw new IllegalArgumentException();
        }
    }

    private static <T extends GeneratedMessageV3> void writeMetaData(Path exportDir, String name, T data) throws IOException {
        writeDataToFile(exportDir, name, META_DATA_SUFFIX, data);
    }

    private static <T extends GeneratedMessageV3> void writeModelData(Path exportDir, String name, T data) throws IOException {
        writeDataToFile(exportDir, name, MODEL_DATA_SUFFIX, data);
    }

    private static <T extends GeneratedMessageV3> void writeDataToFile(Path exportDir, String name, String suffix, T data) throws IOException {
        try (var out = new FileOutputStream(exportDir.resolve(getFileName(name, suffix)).toFile())) {
            data.writeTo(out);
        }
    }

    private static ModelProto.ModelMetaData readMetaData(Path exportDir, String name) throws IOException {
        return ModelProto.ModelMetaData.parseFrom(readDataFromFile(exportDir, name, META_DATA_SUFFIX));
    }

    private static <T> T readModelData(Path exportDir, String name, Parser<T> parser) throws IOException {
        byte[] modelDataBytes = readDataFromFile(exportDir, name, MODEL_DATA_SUFFIX);
        return parser.parseFrom(modelDataBytes);
    }

    private static byte[] readDataFromFile(Path exportDir, String name, String suffix) throws IOException {
        try (var in = new FileInputStream(exportDir.resolve(getFileName(name, suffix)).toFile())) {
            return in.readAllBytes();
        }
    }

    private static String getFileName(String name, String suffix) {
        return formatWithLocale("%s.%s", name, suffix);
    }
}
