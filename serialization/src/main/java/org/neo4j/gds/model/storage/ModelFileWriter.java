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

import com.google.protobuf.GeneratedMessageV3;
import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelMetaDataSerializer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.neo4j.gds.model.storage.ModelToFileExporter.META_DATA_FILE;
import static org.neo4j.gds.model.storage.ModelToFileExporter.MODEL_DATA_FILE;

public class ModelFileWriter<DATA, CONFIG extends BaseConfig & ModelConfig> {

    private final Path persistenceDir;
    private final Model<DATA, CONFIG> model;

    public ModelFileWriter(
        Path persistenceDir,
        Model<DATA, CONFIG> model
    ) {
        this.persistenceDir = persistenceDir;
        this.model = model;
    }

    public void write() throws IOException {
        writeMetaData();
        writeModelData();
    }

    public void writeMetaData() throws IOException {
        var metaDataFile = persistenceDir.resolve(META_DATA_FILE);
        checkFilesExist(metaDataFile);
        writeDataToFile(metaDataFile, ModelMetaDataSerializer.toSerializable(model));
    }

    public void writeModelData() throws IOException {
        var modelDataFile = persistenceDir.resolve(MODEL_DATA_FILE);
        checkFilesExist(modelDataFile);
        writeDataToFile(modelDataFile, toSerializable(model.data(), model.algoType()));
    }

    private GeneratedMessageV3 toSerializable(DATA data, String algoType) throws IOException {
        return ModelSerializerFactory
            .serializer(algoType)
            .toSerializable(data);
    }

    private <T extends GeneratedMessageV3> void writeDataToFile(Path file, T data) throws IOException {
        try (var out = new BufferedOutputStream(Files.newOutputStream(file))) {
            data.writeTo(out);
        }
    }

    private static void checkFilesExist(Path... files) throws FileAlreadyExistsException {
        for (Path file : files) {
            if (Files.exists(file)) {
                throw new FileAlreadyExistsException(file.toAbsolutePath().toString());
            }
        }
    }
}
