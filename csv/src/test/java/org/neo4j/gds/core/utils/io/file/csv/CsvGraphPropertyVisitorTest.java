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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.PropertySchema;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

class CsvGraphPropertyVisitorTest extends CsvVisitorTest {

    @Test
    void shouldExportSingleGraphProperty() {
        var longPropWithSchema = Map.of("longProp", PropertySchema.of("longProp", ValueType.LONG));
        try (var graphPropertyVisitor = new CsvGraphPropertyVisitor(
            tempDir,
            longPropWithSchema,
            new HashSet<>(),
            0
        )) {
            for (int i = 0; i < 4; i++) {
                graphPropertyVisitor.property("longProp", (long) i);
            }
        }

        assertCsvFiles(List.of("graph_properties_longProp_0.csv"));
        assertHeaderFile("graph_properties_longProp_header.csv", longPropWithSchema);
        assertDataContent(
            "graph_properties_longProp_0.csv",
            List.of(
                List.of("0"),
                List.of("1"),
                List.of("2"),
                List.of("3")
            )
        );
    }

    @Override
    protected List<String> defaultHeaderColumns() {
        return List.of();
    }
}