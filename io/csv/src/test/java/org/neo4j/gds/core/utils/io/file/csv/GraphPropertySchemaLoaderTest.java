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
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.PropertySchema;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.core.utils.io.file.CsvSchemaConstants.GRAPH_PROPERTY_SCHEMA_COLUMNS;
import static org.neo4j.gds.core.utils.io.file.csv.CsvGraphPropertySchemaVisitor.GRAPH_PROPERTY_SCHEMA_FILE_NAME;

class GraphPropertySchemaLoaderTest {

    @TempDir Path exportDir;

    @Test
    void shouldLoadGraphPropertySchemaCorrectly() throws IOException {
        var graphPropertySchemaFile = exportDir.resolve(GRAPH_PROPERTY_SCHEMA_FILE_NAME).toFile();
        var lines = List.of(
            String.join(", ", GRAPH_PROPERTY_SCHEMA_COLUMNS),
            "prop1, long, DefaultValue(42), PERSISTENT",
            "prop2, double, DefaultValue(13.37), TRANSIENT"
        );
        FileUtils.writeLines(graphPropertySchemaFile, lines);

        var schemaLoader = new GraphPropertySchemaLoader(exportDir);
        var graphPropertySchema = schemaLoader.load();

        assertThat(graphPropertySchema).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "prop1", PropertySchema.of("prop1", ValueType.LONG, DefaultValue.of(42L), PropertyState.PERSISTENT),
                "prop2", PropertySchema.of("prop2", ValueType.DOUBLE, DefaultValue.of(13.37D), PropertyState.TRANSIENT)
            )
        );
    }

    @Test
    void shouldNotFailOnMissingSchemaFile() {
        var schemaLoader = new GraphPropertySchemaLoader(exportDir);
        var graphPropertySchema = schemaLoader.load();

        assertThat(graphPropertySchema).isEmpty();
    }
}
