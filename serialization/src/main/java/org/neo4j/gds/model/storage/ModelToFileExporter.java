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

import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.ModelConfig;
import org.neo4j.gds.core.model.Model;

import java.io.IOException;
import java.nio.file.Path;

public final class ModelToFileExporter {

    public static final String META_DATA_FILE = "meta";
    public static final String MODEL_DATA_FILE = "data";

    private ModelToFileExporter() {}

    //TODO remove
    public static <DATA, CONFIG extends BaseConfig & ModelConfig, INFO extends Model.Mappable> void toFile(
        Path exportDir,
        Model<DATA, CONFIG, INFO> model
    ) throws IOException {
        new ModelFileWriter<>(exportDir, model).write();
    }
}
