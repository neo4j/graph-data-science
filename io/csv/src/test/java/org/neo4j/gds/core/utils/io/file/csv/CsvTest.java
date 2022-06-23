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

import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.CsvTestSupport;
import org.neo4j.gds.api.schema.PropertySchema;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class CsvTest {

    @TempDir
    protected Path tempDir;

    protected void assertCsvFiles(Collection<String> expectedFiles) {
        CsvTestSupport.assertCsvFiles(tempDir, expectedFiles);
    }

    void assertHeaderFile(
        String fileName,
        List<String> mandatoryColumns,
        Map<String, ? extends PropertySchema> properties
    ) {
        CsvTestSupport.assertHeaderFile(tempDir, fileName, mandatoryColumns, properties);
    }

    void assertDataContent(String fileName, List<List<String>> data) {
        CsvTestSupport.assertDataContent(tempDir, fileName, data);
    }
}

