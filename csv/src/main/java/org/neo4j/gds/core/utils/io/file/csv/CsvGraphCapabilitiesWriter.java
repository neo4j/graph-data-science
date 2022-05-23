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
package org.neo4j.gds.core.utils.io.file.csv;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.StaticCapabilities;

import java.io.IOException;
import java.nio.file.Path;

public class CsvGraphCapabilitiesWriter {

    public static final String GRAPH_CAPABILITIES_FILE_NAME = "graph-capabilities.csv";

    private final ObjectWriter objectWriter;
    private final Path fileLocation;

    public CsvGraphCapabilitiesWriter(Path fileLocation) {
        var csvMapper = new CsvMapper();
        var csvSchema = csvMapper.schemaFor(ImmutableCapabilitiesDTO.class).withHeader();
        this.objectWriter = csvMapper
            .writerFor(ImmutableCapabilitiesDTO.class)
            .with(csvSchema);
        this.fileLocation = fileLocation.resolve(GRAPH_CAPABILITIES_FILE_NAME);
    }

    public void writeCapabilities(Capabilities capabilities) throws IOException {
        var capabilitiesDTO = CapabilitiesDTO.from(capabilities);
        var resultFile = fileLocation.toFile();
        this.objectWriter.writeValue(resultFile, capabilitiesDTO);
    }

    @ValueClass
    @Value.Style(builder = "new")
    @JsonSerialize(as = ImmutableCapabilitiesDTO.class)
    @JsonDeserialize(builder = ImmutableCapabilitiesDTO.Builder.class)
    interface CapabilitiesDTO extends StaticCapabilities {
        static CapabilitiesDTO from(Capabilities capabilities) {
            return new ImmutableCapabilitiesDTO.Builder()
                .from(capabilities)
                .build();
        }
    }
}
