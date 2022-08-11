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
package org.neo4j.gds.api.schema;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GraphSchemaTest {

    @Test
    void testUnionOfGraphPropertiesForDifferentProperties() {
        var prop1Schema = PropertySchema.of("prop1", ValueType.LONG, DefaultValue.of(42L), PropertyState.TRANSIENT);
        var prop2Schema = PropertySchema.of("prop2", ValueType.DOUBLE, DefaultValue.forDouble(), PropertyState.TRANSIENT);

        var graphSchema1 = GraphSchema.of(
            NodeSchema.builder().build(),
            RelationshipSchema.empty(),
            Map.of("prop1", prop1Schema)
        );

        var graphSchema2 = GraphSchema.of(
            NodeSchema.builder().build(),
            RelationshipSchema.empty(),
            Map.of("prop2", prop2Schema)
        );

        var graphSchemaUnion = GraphSchema.of(
            NodeSchema.builder().build(),
            RelationshipSchema.empty(),
            Map.of(
                "prop1", prop1Schema,
                "prop2", prop2Schema
            )
        );

        assertThat(graphSchema1.union(graphSchema2)).isEqualTo(graphSchemaUnion);
    }

    @Test
    void testUnionOfGraphPropertiesForSameProperties() {
        var prop1Schema = PropertySchema.of("prop1", ValueType.LONG, DefaultValue.of(42L), PropertyState.TRANSIENT);

        var graphSchema1 = GraphSchema.of(
            NodeSchema.builder().build(),
            RelationshipSchema.empty(),
            Map.of("prop1", prop1Schema)
        );

        var graphSchema2 = GraphSchema.of(
            NodeSchema.builder().build(),
            RelationshipSchema.empty(),
            Map.of("prop1", prop1Schema)
        );

        assertThat(graphSchema1.union(graphSchema2)).isEqualTo(graphSchema1);
    }

    @Test
    void testUnionOfGraphPropertiesForSamePropertiesWithDifferentTypeThrows() {
        var prop1Schema1 = PropertySchema.of("prop1", ValueType.LONG, DefaultValue.of(42L), PropertyState.TRANSIENT);
        var prop1Schema2 = PropertySchema.of("prop1", ValueType.DOUBLE, DefaultValue.forDouble(), PropertyState.TRANSIENT);

        var graphSchema1 = GraphSchema.of(
            NodeSchema.builder().build(),
            RelationshipSchema.empty(),
            Map.of("prop1", prop1Schema1)
        );

        var graphSchema2 = GraphSchema.of(
            NodeSchema.builder().build(),
            RelationshipSchema.empty(),
            Map.of("prop1", prop1Schema2)
        );

        assertThatThrownBy(() -> graphSchema1.union(graphSchema2))
            .hasMessageContaining("Combining schema entries with value type LONG and DOUBLE is not supported.");
    }

}
