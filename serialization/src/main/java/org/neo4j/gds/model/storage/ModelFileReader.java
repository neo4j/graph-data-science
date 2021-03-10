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
import org.neo4j.gds.model.ModelSupport;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.proto.ModelProto;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;

import static org.neo4j.gds.model.storage.ModelToFileExporter.META_DATA_FILE;
import static org.neo4j.gds.model.storage.ModelToFileExporter.MODEL_DATA_FILE;

public class ModelFileReader {

    private final Path persistenceDir;

    public ModelFileReader(Path persistenceDir) {
        this.persistenceDir = persistenceDir;
    }

    public ModelProto.ModelMetaData readMetaData() throws IOException {
        File file = persistenceDir.resolve(META_DATA_FILE).toFile();
        return ModelProto.ModelMetaData.parseFrom(readDataFromFile(file));
    }

    public Object readData(String algoType) throws IOException {
        return ModelSupport.onValidAlgoType(algoType, () -> {
            var serializer = new GraphSageModelSerializer();
            var parser = serializer.modelParser();
            var graphSageModelProto = readModelData(persistenceDir, parser);
            return serializer.deserializeModelData(graphSageModelProto);
        });
    }

    public Model<?, ?> read() throws IOException {
        var modelMetaData = readMetaData();
        return fromSerializable(modelMetaData);
    }

    private Model<?, ?> fromSerializable(ModelProto.ModelMetaData modelMetaData) throws IOException {
        return ModelSupport.onValidAlgoType(modelMetaData.getAlgoType(), () -> {
            var serializer = new GraphSageModelSerializer();
            var parser = serializer.modelParser();
            var graphSageModelProto = readModelData(persistenceDir, parser);
            return serializer.fromSerializable(graphSageModelProto, modelMetaData);
        });
    }

    private <T> T readModelData(Path exportDir, Parser<T> parser) throws IOException {
        File file = exportDir.resolve(MODEL_DATA_FILE).toFile();
        byte[] modelDataBytes = readDataFromFile(file);
        return parser.parseFrom(modelDataBytes);
    }

    private byte[] readDataFromFile(File file) throws IOException {
        try (var in = new FileInputStream(file)) {
            return in.readAllBytes();
        }
    }
}
