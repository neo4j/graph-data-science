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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.api.schema.PropertySchema;
import org.neo4j.graphalgo.api.schema.RelationshipSchema;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.core.utils.export.file.csv.CsvNodeVisitor.ID_COLUMN_NAME;

class CsvNodeVisitorTest {

    @TempDir
    Path tempDir;

    @Test
    void visitNodesWithoutLabelsAndProperties() {
        var nodeVisitor = new CsvNodeVisitor(tempDir, GraphSchema.empty());

        nodeVisitor.id(0L);
        nodeVisitor.endOfEntity();
        nodeVisitor.id(1L);
        nodeVisitor.endOfEntity();
        nodeVisitor.close();

        assertCsvFiles(List.of("nodes___ALL__.csv", "nodes___ALL___header.csv"));
        assertHeaderFile("nodes___ALL___header.csv", Collections.emptyMap());
        assertDataContent(
            "nodes___ALL__.csv",
            List.of(
                List.of("0"),
                List.of("1")
            )
        );
    }

    @Test
    void visitNodesWithLabels() {
        var nodeVisitor = new CsvNodeVisitor(tempDir, GraphSchema.empty());

        nodeVisitor.id(0L);
        nodeVisitor.labels(new String[]{"Foo", "Bar"});
        nodeVisitor.endOfEntity();
        
        nodeVisitor.id(1L);
        nodeVisitor.labels(new String[]{"Baz"});
        nodeVisitor.endOfEntity();

        nodeVisitor.id(2L);
        nodeVisitor.labels(new String[]{"Foo", "Bar"});
        nodeVisitor.endOfEntity();
        
        nodeVisitor.close();

        assertCsvFiles(List.of("nodes_Bar_Foo.csv", "nodes_Bar_Foo_header.csv", "nodes_Baz.csv", "nodes_Baz_header.csv"));
        assertHeaderFile("nodes_Bar_Foo_header.csv", Collections.emptyMap());
        assertDataContent(
            "nodes_Bar_Foo.csv",
            List.of(
                List.of("0"),
                List.of("2")
            )
        );

        assertHeaderFile("nodes_Baz_header.csv", Collections.emptyMap());
        assertDataContent(
            "nodes_Baz.csv",
            List.of(
                List.of("1")
            )
        );
    }

    @Test
    void visitNodesWithProperties() {
        var graphSchema = GraphSchema.of(
            NodeSchema.builder()
                .addProperty(NodeLabel.ALL_NODES, "foo", ValueType.LONG)
                .addProperty(NodeLabel.ALL_NODES, "bar", ValueType.LONG)
                .build(),
            RelationshipSchema.builder().build()
        );
        var nodeVisitor = new CsvNodeVisitor(tempDir, graphSchema);

        nodeVisitor.id(0L);
        nodeVisitor.property("foo", 42.0);
        nodeVisitor.property("bar", 21.0);
        nodeVisitor.endOfEntity();

        nodeVisitor.id(1L);
        nodeVisitor.property("foo", 42.0);
        nodeVisitor.endOfEntity();

        nodeVisitor.id(2L);
        nodeVisitor.property("bar", 21.0);
        nodeVisitor.endOfEntity();

        nodeVisitor.close();

        assertCsvFiles(List.of("nodes___ALL__.csv", "nodes___ALL___header.csv"));
        assertHeaderFile("nodes___ALL___header.csv", graphSchema.nodeSchema().unionProperties());
        assertDataContent(
            "nodes___ALL__.csv",
            List.of(
                List.of("0", "21.0", "42.0"),
                List.of("1", "", "42.0"),
                List.of("2", "21.0", "")
            )
        );
    }

    private void assertCsvFiles(Collection<String> expectedFiles) {
        assertThat(tempDir.toFile()).isDirectoryContaining(file -> expectedFiles.contains(file.getName()));
        assertThat(tempDir.toFile().listFiles().length).isEqualTo(expectedFiles.size());
    }

    private void assertHeaderFile(String fileName, Map<String, PropertySchema> properties) {
        var expectedContent = new ArrayList<String>();
        expectedContent.add(ID_COLUMN_NAME);

        properties
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach((entry) -> expectedContent.add(entry.getKey() + ":" + entry.getValue().valueType().cypherName()));

        assertThat(tempDir.resolve(fileName)).hasContent(String.join(",", expectedContent));
    }

    private void assertDataContent(String fileName, List<List<String>> data) {
        var expectedContent = data
            .stream()
            .map(row -> String.join(",", row))
            .collect(Collectors.joining("\n"));

        assertThat(tempDir.resolve(fileName)).hasContent(expectedContent);
    }
}
