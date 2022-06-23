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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.List;

import static org.neo4j.gds.core.io.file.NodeSchemaConstants.NODE_SCHEMA_COLUMNS;
import static org.neo4j.gds.core.io.file.csv.CsvNodeSchemaVisitor.NODE_SCHEMA_FILE_NAME;

class CsvNodeSchemaVisitorTest extends CsvVisitorTest {

    @Test
    void writesVisitedNodeSchema() {
        var nodeSchemaVisitor = new CsvNodeSchemaVisitor(tempDir);
        NodeLabel labelA = NodeLabel.of("A");
        nodeSchemaVisitor.nodeLabel(labelA);
        nodeSchemaVisitor.key("prop1");
        nodeSchemaVisitor.valueType(ValueType.LONG);
        nodeSchemaVisitor.defaultValue(DefaultValue.of(42L));
        nodeSchemaVisitor.state(PropertyState.PERSISTENT);
        nodeSchemaVisitor.endOfEntity();

        NodeLabel labelB = NodeLabel.of("B");
        nodeSchemaVisitor.nodeLabel(labelB);
        nodeSchemaVisitor.key("prop2");
        nodeSchemaVisitor.valueType(ValueType.DOUBLE);
        nodeSchemaVisitor.defaultValue(DefaultValue.of(13.37D));
        nodeSchemaVisitor.state(PropertyState.TRANSIENT);
        nodeSchemaVisitor.endOfEntity();

        nodeSchemaVisitor.close();
        assertCsvFiles(List.of(NODE_SCHEMA_FILE_NAME));
        assertDataContent(
            NODE_SCHEMA_FILE_NAME,
            List.of(
                defaultHeaderColumns(),
                List.of("A", "prop1", "long", "DefaultValue(42)", "PERSISTENT"),
                List.of("B", "prop2", "double", "DefaultValue(13.37)", "TRANSIENT")
            )
        );
    }

    @Test
    void writesSchemaWithoutProperties() {
        var nodeSchemaVisitor = new CsvNodeSchemaVisitor(tempDir);
        NodeLabel labelA = NodeLabel.of("A");
        nodeSchemaVisitor.nodeLabel(labelA);
        nodeSchemaVisitor.endOfEntity();

        NodeLabel labelB = NodeLabel.of("B");
        nodeSchemaVisitor.nodeLabel(labelB);
        nodeSchemaVisitor.endOfEntity();

        nodeSchemaVisitor.close();
        assertCsvFiles(List.of(NODE_SCHEMA_FILE_NAME));
        assertDataContent(
            NODE_SCHEMA_FILE_NAME,
            List.of(
                defaultHeaderColumns(),
                List.of("A"),
                List.of("B")
            )
        );
    }

    @Test
    void writesSchemaWithMixedProperties() {
        var nodeSchemaVisitor = new CsvNodeSchemaVisitor(tempDir);
        NodeLabel labelA = NodeLabel.of("A");
        nodeSchemaVisitor.nodeLabel(labelA);
        nodeSchemaVisitor.key("prop1");
        nodeSchemaVisitor.valueType(ValueType.LONG);
        nodeSchemaVisitor.defaultValue(DefaultValue.of(42L));
        nodeSchemaVisitor.state(PropertyState.PERSISTENT);
        nodeSchemaVisitor.endOfEntity();

        NodeLabel labelB = NodeLabel.of("B");
        nodeSchemaVisitor.nodeLabel(labelB);
        nodeSchemaVisitor.endOfEntity();

        nodeSchemaVisitor.close();
        assertCsvFiles(List.of(NODE_SCHEMA_FILE_NAME));
        assertDataContent(
            NODE_SCHEMA_FILE_NAME,
            List.of(
                defaultHeaderColumns(),
                List.of("A", "prop1", "long", "DefaultValue(42)", "PERSISTENT"),
                List.of("B")
            )
        );
    }

    @Override
    protected List<String> defaultHeaderColumns() {
        return NODE_SCHEMA_COLUMNS;
    }
}
