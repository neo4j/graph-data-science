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
package org.neo4j.gds.model.storage;

import com.google.protobuf.Parser;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelSerializer;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.proto.GraphSageProto;
import org.neo4j.graphalgo.core.model.proto.ModelProto;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;

import static org.neo4j.gds.model.storage.ModelToFileExporter.META_DATA_SUFFIX;
import static org.neo4j.gds.model.storage.ModelToFileExporter.MODEL_DATA_SUFFIX;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class ModelFileReader {

    private final Path exportDir;
    private final String fileName;

    public ModelFileReader(Path exportDir, ModelExportConfig config) {
        this.exportDir = exportDir;
        this.fileName = config.fileName();
    }

    public ModelProto.ModelMetaData readMetaData() throws IOException {
        File file = exportDir.resolve(formatWithLocale("%s.%s", fileName, META_DATA_SUFFIX)).toFile();
        return ModelProto.ModelMetaData.parseFrom(readDataFromFile(file));
    }

    public Object readData(String algoType) throws IOException {
        switch (algoType) {
            case GraphSage.MODEL_TYPE:
                var parser = GraphSageProto.GraphSageModel.parser();
                var graphSageModelProto = readModelData(exportDir, fileName, parser);
                return GraphSageModelSerializer.deserializeModelData(graphSageModelProto);
            default:
                throw new IllegalArgumentException();
        }
    }

    public Model<?, ?> read() throws IOException {
        var modelMetaData = readMetaData();
        return fromSerializable(modelMetaData);
    }

    private Model<?, ?> fromSerializable(ModelProto.ModelMetaData modelMetaData) throws IOException {
        switch (modelMetaData.getAlgoType()) {
            case GraphSage.MODEL_TYPE:
                var parser = GraphSageProto.GraphSageModel.parser();
                var graphSageModelProto = readModelData(exportDir, fileName, parser);
                return GraphSageModelSerializer.fromSerializable(graphSageModelProto, modelMetaData);
            default:
                throw new IllegalArgumentException();
        }
    }

    private <T> T readModelData(Path exportDir, String fileName, Parser<T> parser) throws IOException {
        File file = exportDir.resolve(formatWithLocale("%s.%s", fileName, MODEL_DATA_SUFFIX)).toFile();
        byte[] modelDataBytes = readDataFromFile(file);
        return parser.parseFrom(modelDataBytes);
    }

    private byte[] readDataFromFile(File file) throws IOException {
        try (var in = new FileInputStream(file)) {
            return in.readAllBytes();
        }
    }
}
