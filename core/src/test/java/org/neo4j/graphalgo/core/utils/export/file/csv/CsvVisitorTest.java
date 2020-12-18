/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.junit.jupiter.api.io.TempDir;
import org.neo4j.graphalgo.api.schema.PropertySchema;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

abstract public class CsvVisitorTest {

    @TempDir
    Path tempDir;

    abstract List<String> defaultHeaderColumns();

    void assertCsvFiles(Collection<String> expectedFiles) {
        for (String expectedFile : expectedFiles) {
            assertThat(tempDir.toFile()).isDirectoryContaining(file -> file.getName().equals(expectedFile));
        }
    }

    void assertHeaderFile(String fileName, Map<String, ? extends PropertySchema> properties) {
        var expectedContent = new ArrayList<>(defaultHeaderColumns());

        properties
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach((entry) -> expectedContent.add(entry.getKey() + ":" + entry.getValue().valueType().csvName()));

        assertThat(tempDir.resolve(fileName)).hasContent(String.join(",", expectedContent));
    }

    void assertDataContent(String fileName, List<List<String>> data) {
        var expectedContent = data
            .stream()
            .map(row -> String.join(",", row))
            .collect(Collectors.joining("\n"));

        assertThat(tempDir.resolve(fileName)).hasContent(expectedContent);
    }
}
