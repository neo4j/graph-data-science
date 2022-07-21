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

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.core.io.file.csv.CsvGraphInfoVisitor.GRAPH_INFO_FILE_NAME;

class GraphInfoLoaderTest {

    private static final CsvMapper CSV_MAPPER = new CsvMapper();

    @Test
    void shouldLoadGraphInfo(@TempDir Path exportDir) throws IOException {
        var databaseId = DatabaseId.from("my-database");
        var graphInfoFile = exportDir.resolve(GRAPH_INFO_FILE_NAME).toFile();
        var lines = List.of(
            String.join(", ", "databaseName", "nodeCount", "maxOriginalId", "relTypeCounts"),
            String.join(", ", "my-database", "19", "1337", "REL;42")
        );
        FileUtils.writeLines(graphInfoFile, lines);

        var graphInfoLoader = new GraphInfoLoader(exportDir, CSV_MAPPER);
        var graphInfo = graphInfoLoader.load();

        assertThat(graphInfo).isNotNull();
        assertThat(graphInfo.databaseId()).isEqualTo(databaseId);
        assertThat(graphInfo.databaseId().databaseName()).isEqualTo("my-database");

        assertThat(graphInfo.nodeCount()).isEqualTo(19L);
        assertThat(graphInfo.maxOriginalId()).isEqualTo(1337L);

        assertThat(graphInfo.relationshipTypeCounts()).containsExactlyEntriesOf(
            Map.of(RelationshipType.of("REL"), 42L)
        );
    }

    /**
     * Test for backwards compatibility by leaving out `relTypeCounts`
     */
    @Test
    void shouldLoadGraphInfoWithoutRelTypeCounts(@TempDir Path exportDir) throws IOException {
        var graphInfoFile = exportDir.resolve(GRAPH_INFO_FILE_NAME).toFile();
        var lines = List.of(
            String.join(", ", "databaseName", "nodeCount", "maxOriginalId"),
            String.join(", ", "my-database", "19", "1337")
        );
        FileUtils.writeLines(graphInfoFile, lines);

        var graphInfoLoader = new GraphInfoLoader(exportDir, CSV_MAPPER);
        var graphInfo = graphInfoLoader.load();

        assertThat(graphInfo.relationshipTypeCounts()).isEmpty();
    }

}
