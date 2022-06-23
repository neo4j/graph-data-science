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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

class GraphCapabilitiesLoader {

    private final Path capabilitiesPath;
    private final ObjectReader objectReader;

    GraphCapabilitiesLoader(Path csvDirectory, CsvMapper csvMapper) {
        this.capabilitiesPath = csvDirectory.resolve(CsvGraphCapabilitiesWriter.GRAPH_CAPABILITIES_FILE_NAME);

        csvMapper.enable(CsvParser.Feature.TRIM_SPACES);
        csvMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        var schema = CsvSchema.emptySchema().withHeader().withStrictHeaders(false);
        this.objectReader = csvMapper.readerFor(CapabilitiesDTO.class).with(schema);
    }

    Capabilities load() {
        try {
            if (!Files.exists(capabilitiesPath)) {
                return ImmutableStaticCapabilities.builder().build();
            }
            return objectReader.readValue(capabilitiesPath.toFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
