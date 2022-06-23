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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.PropertySchema;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.core.io.file.NodeSchemaConstants.NODE_SCHEMA_COLUMNS;
import static org.neo4j.gds.core.io.file.csv.CsvNodeSchemaVisitor.NODE_SCHEMA_FILE_NAME;

class NodeSchemaLoaderTest {

    @TempDir Path exportDir;

    @Test
    void shouldLoadNodeSchemaCorrectly() throws IOException {
        var nodeSchemaFile = exportDir.resolve(NODE_SCHEMA_FILE_NAME).toFile();
        var lines = List.of(
            String.join(", ", NODE_SCHEMA_COLUMNS),
            "A, prop1, long, DefaultValue(42), PERSISTENT",
            "B, prop2, double, DefaultValue(13.37), TRANSIENT"
        );
        FileUtils.writeLines(nodeSchemaFile, lines);

        var schemaLoader = new NodeSchemaLoader(exportDir);
        var nodeSchema = schemaLoader.load();

        assertThat(nodeSchema).isNotNull();

        assertThat(nodeSchema.availableLabels()).containsExactlyInAnyOrder(NodeLabel.of("A"), NodeLabel.of("B"));

        var labelAProperties = nodeSchema.filterProperties(Set.of(NodeLabel.of("A")));
        assertThat(labelAProperties)
            .containsExactlyInAnyOrderEntriesOf(Map.of(
                NodeLabel.of("A"),
                Map.of(
                    "prop1",
                    PropertySchema.of(
                        "prop1",
                        ValueType.LONG,
                        DefaultValue.of(42L),
                        PropertyState.PERSISTENT
                    )
                )
            ));

        var labelBProperties = nodeSchema.filterProperties(Set.of(NodeLabel.of("B")));
        assertThat(labelBProperties)
            .containsExactlyInAnyOrderEntriesOf(Map.of(
                NodeLabel.of("B"),
                Map.of(
                    "prop2",
                    PropertySchema.of(
                        "prop2",
                        ValueType.DOUBLE,
                        DefaultValue.of(13.37D),
                        PropertyState.TRANSIENT
                    )
                )
            ));
    }

    @Test
    void shouldLoadSchemaWithoutProperties() throws IOException {
        var nodeSchemaFile = exportDir.resolve(NODE_SCHEMA_FILE_NAME).toFile();
        var lines = List.of(
            String.join(", ", NODE_SCHEMA_COLUMNS),
            "A",
            "B"
        );
        FileUtils.writeLines(nodeSchemaFile, lines);

        var schemaLoader = new NodeSchemaLoader(exportDir);
        var nodeSchema = schemaLoader.load();

        assertThat(nodeSchema).isNotNull();
        assertThat(nodeSchema.availableLabels()).containsExactlyInAnyOrder(NodeLabel.of("A"), NodeLabel.of("B"));
    }

    @Test
    void shouldLoadMixedLabels() throws IOException {
        var nodeSchemaFile = exportDir.resolve(NODE_SCHEMA_FILE_NAME).toFile();
        var lines = List.of(
            String.join(", ", NODE_SCHEMA_COLUMNS),
            "A, prop1, long, DefaultValue(42), PERSISTENT",
            "B"
        );
        FileUtils.writeLines(nodeSchemaFile, lines);

        var schemaLoader = new NodeSchemaLoader(exportDir);
        var nodeSchema = schemaLoader.load();

        assertThat(nodeSchema).isNotNull();

        assertThat(nodeSchema.availableLabels()).containsExactlyInAnyOrder(NodeLabel.of("A"), NodeLabel.of("B"));

        var labelAProperties = nodeSchema.filterProperties(Set.of(NodeLabel.of("A")));
        assertThat(labelAProperties)
            .containsExactlyInAnyOrderEntriesOf(Map.of(
                NodeLabel.of("A"),
                Map.of(
                    "prop1",
                    PropertySchema.of(
                        "prop1",
                        ValueType.LONG,
                        DefaultValue.of(42L),
                        PropertyState.PERSISTENT
                    )
                )
            ));

        var labelBProperties = nodeSchema.filterProperties(Set.of(NodeLabel.of("B")));
        assertThat(labelBProperties)
            .containsExactlyInAnyOrderEntriesOf(Map.of(NodeLabel.of("B"), Map.of()));
    }

}
