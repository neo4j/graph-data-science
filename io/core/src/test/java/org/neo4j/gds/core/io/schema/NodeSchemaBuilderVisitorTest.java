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
package org.neo4j.gds.core.io.schema;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.NodeSchemaEntry;
import org.neo4j.gds.api.schema.PropertySchema;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NodeSchemaBuilderVisitorTest {

    @Test
    void shouldBuildNodeSchema() {
        var nodeSchemaBuilderVisitor = new NodeSchemaBuilderVisitor();
        NodeLabel labelA = NodeLabel.of("A");
        nodeSchemaBuilderVisitor.nodeLabel(labelA);
        nodeSchemaBuilderVisitor.key("prop1");
        nodeSchemaBuilderVisitor.valueType(ValueType.LONG);
        nodeSchemaBuilderVisitor.defaultValue(DefaultValue.of(42L));
        nodeSchemaBuilderVisitor.state(PropertyState.PERSISTENT);
        nodeSchemaBuilderVisitor.endOfEntity();

        NodeLabel labelB = NodeLabel.of("B");
        nodeSchemaBuilderVisitor.nodeLabel(labelB);
        nodeSchemaBuilderVisitor.key("prop2");
        nodeSchemaBuilderVisitor.valueType(ValueType.DOUBLE);
        nodeSchemaBuilderVisitor.defaultValue(DefaultValue.of(13.37D));
        nodeSchemaBuilderVisitor.state(PropertyState.TRANSIENT);
        nodeSchemaBuilderVisitor.endOfEntity();

        nodeSchemaBuilderVisitor.close();

        var builtSchema = nodeSchemaBuilderVisitor.schema();

        assertThat(builtSchema).isNotNull();
        assertThat(builtSchema.availableLabels()).containsExactlyInAnyOrder(labelA, labelB);

        var labelAEntry = builtSchema.get(NodeLabel.of("A"));
        assertThat(labelAEntry)
            .isEqualTo(
                new NodeSchemaEntry(
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
                )
            );

        var labelBEntry = builtSchema.get(NodeLabel.of("B"));
        assertThat(labelBEntry)
            .isEqualTo(
                new NodeSchemaEntry(
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
                )
            );
    }

}
