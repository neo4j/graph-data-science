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
package org.neo4j.gds;

import org.neo4j.gds.api.schema.PropertySchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public final class CsvTestSupport {

    private CsvTestSupport() {}

    public static void assertCsvFiles(Path path, Iterable<String> expectedFiles) {
        for (var expectedFile : expectedFiles) {
            assertThat(path).isDirectoryContaining("glob:**/" + expectedFile);
        }
    }

    public static void assertHeaderFile(
        Path path,
        String fileName,
        List<String> mandatoryColumns,
        Map<String, ? extends PropertySchema> properties
    ) {
        var expectedContent = new ArrayList<>(mandatoryColumns);

        properties
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> {
                var propAndType = entry.getKey() + ":" + entry.getValue().valueType().csvName();
                // magic value from CsvEncoder.MAX_QUOTE_CHECK
                if (propAndType.length() > 24) {
                    propAndType = "\"" + propAndType + "\"";
                }
                expectedContent.add(propAndType);
            });

        assertThat(path.resolve(fileName)).hasContent(String.join(",", expectedContent));
    }

    public static void assertDataContent(Path path, String fileName, Collection<List<String>> data) {
        var expectedContent = data
            .stream()
            .map(row -> String.join(",", row))
            .collect(Collectors.toList());

        try {
            var fileLines = Files.readAllLines(path.resolve(fileName), StandardCharsets.UTF_8);
            assertThat(fileLines).containsExactlyInAnyOrderElementsOf(expectedContent);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
