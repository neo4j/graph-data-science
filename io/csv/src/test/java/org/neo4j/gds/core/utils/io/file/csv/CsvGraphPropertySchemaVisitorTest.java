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
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.List;

import static org.neo4j.gds.core.utils.io.file.CsvSchemaConstants.GRAPH_PROPERTY_SCHEMA_COLUMNS;
import static org.neo4j.gds.core.utils.io.file.csv.CsvGraphPropertySchemaVisitor.GRAPH_PROPERTY_SCHEMA_FILE_NAME;

class CsvGraphPropertySchemaVisitorTest extends CsvVisitorTest {

    @Test
    void writesVisitedGraphPropertySchema() {
        var graphPropertySchemaVisitor = new CsvGraphPropertySchemaVisitor(tempDir);
        graphPropertySchemaVisitor.key("prop1");
        graphPropertySchemaVisitor.valueType(ValueType.LONG);
        graphPropertySchemaVisitor.defaultValue(DefaultValue.of(1337L));
        graphPropertySchemaVisitor.state(PropertyState.TRANSIENT);
        graphPropertySchemaVisitor.endOfEntity();

        graphPropertySchemaVisitor.key("prop2");
        graphPropertySchemaVisitor.valueType(ValueType.DOUBLE);
        graphPropertySchemaVisitor.defaultValue(DefaultValue.of(42.0D));
        graphPropertySchemaVisitor.state(PropertyState.PERSISTENT);
        graphPropertySchemaVisitor.endOfEntity();

        graphPropertySchemaVisitor.close();

        assertCsvFiles(List.of(GRAPH_PROPERTY_SCHEMA_FILE_NAME));
        assertDataContent(
            GRAPH_PROPERTY_SCHEMA_FILE_NAME,
            List.of(
                defaultHeaderColumns(),
                List.of("prop1", "long", "DefaultValue(1337)", "TRANSIENT"),
                List.of("prop2", "double", "DefaultValue(42.0)", "PERSISTENT")
            )
        );
    }

    @Override
    protected List<String> defaultHeaderColumns() {
        return GRAPH_PROPERTY_SCHEMA_COLUMNS;
    }
}
