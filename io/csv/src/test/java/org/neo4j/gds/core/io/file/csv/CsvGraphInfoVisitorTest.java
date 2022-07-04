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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.io.file.ImmutableGraphInfo;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.core.io.file.csv.CsvGraphInfoVisitor.GRAPH_INFO_FILE_NAME;

class CsvGraphInfoVisitorTest extends CsvVisitorTest {

    @Test
    void shouldExportGraphInfo() {
        NamedDatabaseId namedDatabaseId = Neo4jProxy.randomDatabaseId();
        CsvGraphInfoVisitor graphInfoVisitor = new CsvGraphInfoVisitor(tempDir);
        var relationshipTypeCounts = Map.of(RelationshipType.of("REL1"), 42L, RelationshipType.of("REL2"), 1337L);
        graphInfoVisitor.export(ImmutableGraphInfo.of(namedDatabaseId, 1337L, 19L, relationshipTypeCounts));
        graphInfoVisitor.close();

        assertCsvFiles(List.of(GRAPH_INFO_FILE_NAME));
        assertDataContent(
            GRAPH_INFO_FILE_NAME,
            List.of(
                defaultHeaderColumns(),
                List.of(
                    namedDatabaseId.databaseId().uuid().toString(),
                    namedDatabaseId.name(),
                    Long.toString(1337L),
                    Long.toString(19L),
                    CsvMapUtil.relationshipCountsToString(relationshipTypeCounts)
                )
            )
        );
    }

    @Override
    protected List<String> defaultHeaderColumns() {
        return List.of(
            CsvGraphInfoVisitor.DATABASE_ID_COLUMN_NAME,
            CsvGraphInfoVisitor.DATABASE_NAME_COLUMN_NAME,
            CsvGraphInfoVisitor.NODE_COUNT_COLUMN_NAME,
            CsvGraphInfoVisitor.MAX_ORIGINAL_ID_COLUMN_NAME,
            CsvGraphInfoVisitor.REL_TYPE_COUNTS_COLUMN_NAME
        );
    }
}
