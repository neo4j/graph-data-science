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

import com.google.protobuf.Parser;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelSerializer;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.proto.GraphSageProto;
import org.neo4j.graphalgo.core.model.proto.ModelProto;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class ModelToFileExporter {

    static final String META_DATA_SUFFIX = "meta";
    static final String MODEL_DATA_SUFFIX = "data";

    private ModelToFileExporter() {}

    public static <DATA, CONFIG extends BaseConfig & ModelConfig> void toFile(
        Path exportDir,
        Model<DATA, CONFIG> model,
        ModelExportConfig config
    ) throws IOException {
        new ModelFileWriter<>(exportDir, model, config).write();
    }

    public static <DATA, CONFIG extends BaseConfig & ModelConfig> Model<DATA, CONFIG> fromFile(Path exportDir, String fileName) throws
        IOException, ClassNotFoundException {
        var modelMetaData = readMetaData(exportDir, fileName);
        return fromSerializable(exportDir, fileName, modelMetaData);
    }

    private static <DATA, CONFIG extends BaseConfig & ModelConfig> Model<DATA, CONFIG> fromSerializable(
        Path exportDir,
        String fileName,
        ModelProto.ModelMetaData modelMetaData
    ) throws IOException, ClassNotFoundException {
        switch (modelMetaData.getAlgoType()) {
            case GraphSage.MODEL_TYPE:
                var parser = GraphSageProto.GraphSageModel.parser();
                var graphSageModelProto = readModelData(exportDir, fileName, parser);
                return (Model<DATA, CONFIG>) GraphSageModelSerializer.fromSerializable(graphSageModelProto, modelMetaData);
            default:
                throw new IllegalArgumentException();
        }
    }

    private static ModelProto.ModelMetaData readMetaData(Path exportDir, String fileName) throws IOException {
        return ModelProto.ModelMetaData.parseFrom(readDataFromFile(exportDir, fileName, META_DATA_SUFFIX));
    }

    private static <T> T readModelData(Path exportDir, String fileName, Parser<T> parser) throws IOException {
        byte[] modelDataBytes = readDataFromFile(exportDir, fileName, MODEL_DATA_SUFFIX);
        return parser.parseFrom(modelDataBytes);
    }

    private static byte[] readDataFromFile(Path exportDir, String fileName, String suffix) throws IOException {
        try (var in = new FileInputStream(exportDir.resolve(formatWithLocale("%s.%s", fileName, suffix)).toFile())) {
            return in.readAllBytes();
        }
    }

}
