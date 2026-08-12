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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.neo4j.gds.core.loading.Capabilities;
import tools.jackson.databind.ObjectReader;
import tools.jackson.dataformat.csv.CsvMapper;
import tools.jackson.dataformat.csv.CsvReadFeature;
import tools.jackson.dataformat.csv.CsvSchema;

import java.nio.file.Files;
import java.nio.file.Path;

public class GraphCapabilitiesLoader {

    private final Path capabilitiesPath;
    private final ObjectReader objectReader;

    public GraphCapabilitiesLoader(Path csvDirectory, CsvMapper csvMapper) {
        this.capabilitiesPath = csvDirectory.resolve(CsvGraphCapabilitiesWriter.GRAPH_CAPABILITIES_FILE_NAME);

        var mapper = csvMapper.rebuild()
            .enable(CsvReadFeature.TRIM_SPACES)
            .build();
        var schema = CsvSchema.emptySchema().withHeader().withStrictHeaders(false);
        this.objectReader = mapper.readerFor(CapabilitiesLine.class).with(schema);
    }

    public Capabilities load() {
        if (!Files.exists(capabilitiesPath)) {
            return new Capabilities();
        }
        var line = objectReader.<CapabilitiesLine>readValue(capabilitiesPath.toFile());
        return new Capabilities(line.writeMode);
    }

    private static class CapabilitiesLine {
        @JsonProperty
        Capabilities.WriteMode writeMode = Capabilities.WriteMode.LOCAL;
    }
}
