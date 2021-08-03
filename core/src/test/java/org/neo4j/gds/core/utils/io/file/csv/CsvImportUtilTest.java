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

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.utils.io.file.HeaderProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    @MethodSource("nodeFileNames")
    void shouldFindNodeFiles(List<String> fileNames) throws IOException {
        for (String fileName : fileNames) {
            Files.createFile(tempDir.resolve(fileName));
        }
        var nodeHeaderFiles = CsvImportUtil.getNodeHeaderFiles(tempDir)
            .stream()
            .map(Path::getFileName)
            .map(Path::toString)
            .collect(Collectors.toList());

        assertThat(nodeHeaderFiles).containsExactlyInAnyOrder(
            "nodes_header.csv",
            "nodes_A_B_header.csv",
            "nodes_A_C_header.csv",
            "nodes_B_header.csv",
            "nodes_Person_header.csv",
            "nodes_House_Property_header.csv"
        );
    }

    @ParameterizedTest
    @MethodSource("nodeFileNames")
    void shouldConstructNodeHeaderToDataFileMapping(List<String> fileNames) throws IOException {
        for (String fileName : fileNames) {
            Files.createFile(tempDir.resolve(fileName));
        }
        Map<Path, List<Path>> headerToFileMapping = CsvImportUtil.nodeHeaderToFileMapping(tempDir);
        headerToFileMapping.values().forEach(paths -> paths.sort(Comparator.comparing(Path::toString)));

        Map<Path, List<Path>> expectedMapping = Map.of(
            tempDir.resolve("nodes_header.csv"), List.of(tempDir.resolve("nodes_0.csv")),
            tempDir.resolve("nodes_A_B_header.csv"), List.of(tempDir.resolve("nodes_A_B_0.csv"), tempDir.resolve("nodes_A_B_1.csv")),
            tempDir.resolve("nodes_A_C_header.csv"), List.of(tempDir.resolve("nodes_A_C_0.csv"), tempDir.resolve("nodes_A_C_1.csv")),
            tempDir.resolve("nodes_B_header.csv"), List.of(tempDir.resolve("nodes_B_0.csv"), tempDir.resolve("nodes_B_1.csv"), tempDir.resolve("nodes_B_2.csv")),
            tempDir.resolve("nodes_Person_header.csv"), List.of(),
            tempDir.resolve("nodes_House_Property_header.csv"), List.of()
        );
        assertThat(headerToFileMapping).containsExactlyInAnyOrderEntriesOf(expectedMapping);
    }

    @ParameterizedTest
    @MethodSource("relationshipsFileNames")
    void shouldFindRelationshipHeaderFiles(List<String> fileNames) throws IOException {
        for (String fileName : fileNames) {
            Files.createFile(tempDir.resolve(fileName));
        }
        var relationshipHeaderFiles = CsvImportUtil.getRelationshipHeaderFiles(tempDir)
            .stream()
            .map(Path::getFileName)
            .map(Path::toString)
            .collect(Collectors.toList());

        assertThat(relationshipHeaderFiles).containsExactlyInAnyOrder("relationships_REL_header.csv", "relationships_REL1_header.csv", "relationships_REL2_header.csv");
    }

    @ParameterizedTest
    @MethodSource("relationshipsFileNames")
    void shouldConstructRelationshipHeaderToDataFileMapping(List<String> fileNames) throws IOException {
        for (String fileName : fileNames) {
            Files.createFile(tempDir.resolve(fileName));
        }
        Map<Path, List<Path>> relationshipHeaderToFileMapping = CsvImportUtil.relationshipHeaderToFileMapping(tempDir);
        relationshipHeaderToFileMapping.values().forEach(paths -> paths.sort(Comparator.comparing(Path::toString)));

        Map<Path, List<Path>> expectedMapping = Map.of(
            tempDir.resolve("relationships_REL_header.csv"), List.of(tempDir.resolve("relationships_REL_0.csv"), tempDir.resolve("relationships_REL_1.csv")),
            tempDir.resolve("relationships_REL1_header.csv"), List.of(tempDir.resolve("relationships_REL1_0.csv"), tempDir.resolve("relationships_REL1_1.csv")),
            tempDir.resolve("relationships_REL2_header.csv"), List.of(tempDir.resolve("relationships_REL2_0.csv"), tempDir.resolve("relationships_REL2_1.csv"), tempDir.resolve("relationships_REL2_2.csv"))
        );
        assertThat(relationshipHeaderToFileMapping).containsExactlyInAnyOrderEntriesOf(expectedMapping);
    }

    @Test
    void shouldParseNodeHeaderFile() throws IOException {
        var headerPath = tempDir.resolve("nodes_Person_King_header.csv");
        FileUtils.writeLines(headerPath.toFile(), List.of(":ID,foo:long,bar:double"));

        var parsedHeader = CsvImportUtil.parseNodeHeader(headerPath);

        assertThat(parsedHeader.nodeLabels()).containsExactlyInAnyOrder("Person", "King");
        assertThat(parsedHeader.propertyMappings()).containsExactlyInAnyOrder(
            HeaderProperty.parse(1, "foo:long"),
            HeaderProperty.parse(2, "bar:double")
        );
    }

    @Test
    void shouldParseRelationshipHeaderFile() throws IOException {
        var headerPath = tempDir.resolve("relationships_R_header.csv");
        FileUtils.writeLines(headerPath.toFile(), List.of(":START_ID,:END_ID,foo:long,bar:double"));

        var parsedHeader = CsvImportUtil.parseRelationshipHeader(headerPath);

        assertThat(parsedHeader.relationshipType()).isEqualTo("R");
        assertThat(parsedHeader.propertyMappings()).containsExactlyInAnyOrder(
            HeaderProperty.parse(2, "foo:long"),
            HeaderProperty.parse(3, "bar:double")
        );
    }

    @Test
    void shouldReturnEmptyArrayForNoLabels() {
        var noLabelsNodeHeaderFileName = "nodes_header.csv";
        assertThat(CsvImportUtil.inferNodeLabels(noLabelsNodeHeaderFileName)).isEmpty();
        assertThat(CsvImportUtil.inferNodeLabels(Paths.get(noLabelsNodeHeaderFileName))).isEqualTo(new String[0]);
    }

    static Stream<Arguments> nodeFileNames() {
        return Stream.of(
            Arguments.of(
                List.of(
                    "nodes_0.csv",
                    "nodes_A_B_0.csv",
                    "nodes_A_B_1.csv",
                    "nodes_A_C_0.csv",
                    "nodes_A_C_1.csv",
                    "nodes_B_0.csv",
                    "nodes_B_1.csv",
                    "nodes_B_2.csv",
                    "nodes_header.csv",
                    "nodes_A_B_header.csv",
                    "nodes_A_C_header.csv",
                    "nodes_Person_header.csv",
                    "nodes_House_Property_header.csv",
                    "nodes_B_header.csv"
                )
            )
        );
    }

    static Stream<Arguments> relationshipsFileNames() {
        return Stream.of(
            Arguments.of(
                List.of(
                    "relationships_REL_0.csv",
                    "relationships_REL_1.csv",
                    "relationships_REL1_0.csv",
                    "relationships_REL1_1.csv",
                    "relationships_REL2_0.csv",
                    "relationships_REL2_1.csv",
                    "relationships_REL2_2.csv",
                    "relationships_REL_header.csv",
                    "relationships_REL1_header.csv",
                    "relationships_REL2_header.csv"
                )
            )
        );
    }
}
