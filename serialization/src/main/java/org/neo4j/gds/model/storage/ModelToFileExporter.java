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
import org.neo4j.gds.embeddings.graphsage.GraphSageModelSerializer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelMetaDataSerializer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class ModelToFileExporter {

    public static final String META_DATA_SUFFIX = "meta";
    public static final String MODEL_DATA_SUFFIX = "data";

    private ModelToFileExporter() {}

    public static <DATA, CONFIG extends BaseConfig & ModelConfig> void toFile(Model<DATA, CONFIG> model, Path exportDir) throws IOException {
        // TODO: some validation, blah-blah
        writeMetaData(exportDir, model.name(), ModelMetaDataSerializer.toSerializable(model));
        writeModelData(exportDir, model.name(), toSerializable(model.data(), model.algoType()));
    }

    private static <DATA> GeneratedMessageV3 toSerializable(DATA data, String algoType) throws IOException {
        switch (algoType) {
            case GraphSage.MODEL_TYPE:
                return GraphSageModelSerializer.toSerializable((ModelData) data);
            default:
                throw new IllegalArgumentException(formatWithLocale("Algo type %s was not found.", algoType));
        }
    }

    private static <T extends GeneratedMessageV3> void writeMetaData(Path exportDir, String name, T data) throws IOException {
        writeDataToFile(exportDir, name, META_DATA_SUFFIX, data);
    }

    private static <T extends GeneratedMessageV3> void writeModelData(Path exportDir, String name, T data) throws IOException {
        writeDataToFile(exportDir, name, MODEL_DATA_SUFFIX, data);
    }

    private static <T extends GeneratedMessageV3> void writeDataToFile(Path exportDir, String name, String suffix, T data) throws IOException {
        var fileName = formatWithLocale("%s.%s", name, suffix);
        try(var out = new FileOutputStream(exportDir.resolve(fileName).toFile())) {
            data.writeTo(out);
        }
    }
}
