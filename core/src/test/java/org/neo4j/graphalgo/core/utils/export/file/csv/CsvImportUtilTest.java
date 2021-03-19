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

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CsvImportUtilTest {

    @TempDir
    Path tempDir;

    @ParameterizedTest
    @MethodSource("fileNames")
    void shouldFindNodeFiles(List<String> fileNames) throws IOException {
        for (String fileName : fileNames) {
            Files.createFile(tempDir.resolve(fileName));
        }
        var nodeHeaderFiles = Arrays
            .stream(CsvImportUtil.getNodeHeaderFiles(tempDir))
            .map(File::getName)
            .collect(Collectors.toList());

        assertThat(nodeHeaderFiles).hasSize(3);
        assertThat(nodeHeaderFiles).containsExactlyInAnyOrder("nodes_A_B_header.csv", "nodes_A_C_header.csv", "nodes_B_header.csv");
    }

    @ParameterizedTest
    @MethodSource("fileNames")
    void shouldConstructHeaderToDataFileMapping(List<String> fileNames) throws IOException {
        for (String fileName : fileNames) {
            Files.createFile(tempDir.resolve(fileName));
        }
        Map<Path, List<Path>> headerToFileMapping = CsvImportUtil.headerToFileMapping(tempDir);
        headerToFileMapping.values().forEach(paths -> paths.sort(Comparator.comparing(Path::toString)));

        assertThat(headerToFileMapping).hasSize(3);
        Map<Path, List<Path>> expectedMapping = Map.of(
            tempDir.resolve("nodes_A_B_header.csv"), List.of(tempDir.resolve("nodes_A_B_0.csv"), tempDir.resolve("nodes_A_B_1.csv")),
            tempDir.resolve("nodes_A_C_header.csv"), List.of(tempDir.resolve("nodes_A_C_0.csv"), tempDir.resolve("nodes_A_C_1.csv")),
            tempDir.resolve("nodes_B_header.csv"), List.of(tempDir.resolve("nodes_B_0.csv"), tempDir.resolve("nodes_B_1.csv"), tempDir.resolve("nodes_B_2.csv"))
        );
        assertThat(headerToFileMapping).containsExactlyInAnyOrderEntriesOf(expectedMapping);
    }

    static Stream<Arguments> fileNames() {
        return Stream.of(
            Arguments.of(
                List.of(
                    "nodes_A_B_0.csv",
                    "nodes_A_B_1.csv",
                    "nodes_A_C_0.csv",
                    "nodes_A_C_1.csv",
                    "nodes_B_0.csv",
                    "nodes_B_1.csv",
                    "nodes_B_2.csv",
                    "nodes_A_B_header.csv",
                    "nodes_A_C_header.csv",
                    "nodes_B_header.csv"
                )
            )
        );
    }

}