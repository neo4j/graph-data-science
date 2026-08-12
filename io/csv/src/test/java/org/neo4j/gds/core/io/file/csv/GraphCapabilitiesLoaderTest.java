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

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.core.loading.Capabilities;
import tools.jackson.dataformat.csv.CsvMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.core.io.file.csv.CsvGraphCapabilitiesWriter.GRAPH_CAPABILITIES_FILE_NAME;

public class GraphCapabilitiesLoaderTest {

    private static final CsvMapper CSV_MAPPER = new CsvMapper();

    @TempDir
    Path exportDir;

    @ParameterizedTest
    @EnumSource(Capabilities.WriteMode.class)
    void shouldLoadGraphCapabilities(Capabilities.WriteMode writeMode) throws IOException {
        var graphCapabilitiesFile = exportDir.resolve(GRAPH_CAPABILITIES_FILE_NAME).toFile();

        var lines = List.of(
            "writeMode",
            writeMode.name()
        );
        FileUtils.writeLines(graphCapabilitiesFile, lines);

        var capabilitiesLoader = new GraphCapabilitiesLoader(exportDir, CSV_MAPPER);
        var capabilities = capabilitiesLoader.load();

        assertThat(capabilities.canWriteToLocalDatabase()).isEqualTo(writeMode == Capabilities.WriteMode.LOCAL);
        assertThat(capabilities.canWriteToRemoteDatabase()).isEqualTo(writeMode == Capabilities.WriteMode.REMOTE);
    }

    @Test
    void shouldLoadDefaultCapabilitiesForMissingFile() {
        var capabilitiesLoader = new GraphCapabilitiesLoader(exportDir, CSV_MAPPER);
        var capabilities = capabilitiesLoader.load();

        assertThat(capabilities).isEqualTo(new Capabilities());
    }

    @Test
    void shouldLoadDefaultCapabilitiesUnknownEntriesFile() throws IOException {
        var graphCapabilitiesFile = exportDir.resolve(GRAPH_CAPABILITIES_FILE_NAME).toFile();
        FileUtils.writeLines(graphCapabilitiesFile, List.of("an-unknown-feature", "foo"));

        var capabilitiesLoader = new GraphCapabilitiesLoader(exportDir, CSV_MAPPER);
        var capabilities = capabilitiesLoader.load();

        assertThat(capabilities)
            .usingRecursiveComparison()
            .isEqualTo(new Capabilities());
    }
}
