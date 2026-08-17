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

import org.neo4j.gds.core.io.file.SimpleWriter;
import org.neo4j.gds.core.loading.Capabilities;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.dataformat.csv.CsvMapper;

import java.nio.file.Path;

public class CsvGraphCapabilitiesWriter implements SimpleWriter<Capabilities> {

    public static final String GRAPH_CAPABILITIES_FILE_NAME = "graph-capabilities.csv";

    private final ObjectWriter objectWriter;
    private final Path fileLocation;

    public CsvGraphCapabilitiesWriter(Path fileLocation) {
        var csvMapper = new CsvMapper();
        var csvSchema = csvMapper.schemaFor(Capabilities.class).withHeader();
        this.objectWriter = csvMapper.writerFor(Capabilities.class).with(csvSchema);
        this.fileLocation = fileLocation.resolve(GRAPH_CAPABILITIES_FILE_NAME);
    }

    @Override
    public void write(Capabilities capabilities) {
        var resultFile = fileLocation.toFile();
        this.objectWriter.writeValue(resultFile, capabilities);
    }
}
