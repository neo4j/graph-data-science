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
package org.neo4j.graphalgo.core.utils.export.file.csv;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.export.file.ImmutableGraphInfo;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import java.util.List;

import static org.neo4j.graphalgo.core.utils.export.file.csv.CsvGraphInfoVisitor.GRAPH_INFO_FILE_NAME;

class CsvGraphInfoVisitorTest extends CsvVisitorTest {

    @Test
    void shouldExportDatabaseId() {
        NamedDatabaseId namedDatabaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
        CsvGraphInfoVisitor graphInfoVisitor = new CsvGraphInfoVisitor(tempDir);
        graphInfoVisitor.export(ImmutableGraphInfo.of(namedDatabaseId, 1337L));
        graphInfoVisitor.close();

        assertCsvFiles(List.of(GRAPH_INFO_FILE_NAME));
        assertDataContent(
            GRAPH_INFO_FILE_NAME,
            List.of(
                defaultHeaderColumns(),
                List.of(namedDatabaseId.databaseId().uuid().toString(), namedDatabaseId.name(), Long.toString(1337L))
            )
        );
    }

    @Override
    protected List<String> defaultHeaderColumns() {
        return List.of(CsvGraphInfoVisitor.DATABASE_ID_COLUMN_NAME, CsvGraphInfoVisitor.DATABASE_NAME_COLUMN_NAME, CsvGraphInfoVisitor.NODE_COUNT_COLUMN_NAME);
    }
}
