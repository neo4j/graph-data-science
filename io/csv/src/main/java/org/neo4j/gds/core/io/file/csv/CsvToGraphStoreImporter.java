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
package org.neo4j.gds.core.io.file.csv;

import org.neo4j.gds.core.io.file.FileInput;
import org.neo4j.gds.core.io.file.FileToGraphStoreImporter;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.logging.Log;

import java.nio.file.Path;

public class CsvToGraphStoreImporter extends FileToGraphStoreImporter {

    public CsvToGraphStoreImporter(
        int concurrency,
        Path importPath,
        Log log,
        TaskRegistryFactory taskRegistryFactory
    ) {
        super(concurrency, importPath, log, taskRegistryFactory);
    }

    @Override
    protected FileInput fileInput(Path importPath) {
        return new CsvFileInput(importPath);
    }
}
