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
package org.neo4j.gds.core.loading.construction;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NodeLabelTokenToPropertyKeysTest {

    @Test
    void testPropertySchemasLazy() {
        var mapping = NodeLabelTokenToPropertyKeys.lazy();

        mapping.add(NodeLabelTokens.ofStrings("A"), List.of("foo", "bar"));
        mapping.add(NodeLabelTokens.ofStrings("A"), List.of("baz"));
        mapping.add(NodeLabelTokens.ofStrings("B"), List.of("baz", "bob"));
        mapping.add(NodeLabelTokens.ofStrings("C"), List.of());

        var importPropertySchemas = Map.of(
            "foo", PropertySchema.of("foo", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT),
            "bar", PropertySchema.of("bar", ValueType.DOUBLE, DefaultValue.forDouble(), PropertyState.PERSISTENT),
            "baz", PropertySchema.of("baz", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT),
            "bob", PropertySchema.of("bob", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT)
        );

        assertThat(mapping.propertySchemas(NodeLabel.of("A"), importPropertySchemas)).isEqualTo(Map.of(
            "foo", PropertySchema.of("foo", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT),
            "bar", PropertySchema.of("bar", ValueType.DOUBLE, DefaultValue.forDouble(), PropertyState.PERSISTENT),
            "baz", PropertySchema.of("baz", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT)
        ));
        assertThat(mapping.propertySchemas(NodeLabel.of("B"), importPropertySchemas)).isEqualTo(Map.of(
            "baz", PropertySchema.of("baz", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT),
            "bob", PropertySchema.of("bob", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT)
        ));
        assertThat(mapping.propertySchemas(NodeLabel.of("C"), importPropertySchemas)).isEqualTo(Map.of());
    }

    @Test
    void testPropertySchemasFixed() {
        var nodeSchema = NodeSchema.empty()
            .addLabel(NodeLabel.of("A"), Map.of(
                "foo", PropertySchema.of("foo", ValueType.LONG),
                "bar", PropertySchema.of("bar", ValueType.DOUBLE),
                "baz", PropertySchema.of("baz", ValueType.LONG)
            ))
            .addLabel(NodeLabel.of("B"), Map.of(
                "baz", PropertySchema.of("baz", ValueType.LONG),
                "bob", PropertySchema.of("bob", ValueType.LONG)
            ))
            .addLabel(NodeLabel.of("C"));

        var importPropertySchemas = Map.of(
            "foo", PropertySchema.of("foo", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT),
            "bar", PropertySchema.of("bar", ValueType.DOUBLE, DefaultValue.forDouble(), PropertyState.PERSISTENT),
            "baz", PropertySchema.of("baz", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT),
            "bob", PropertySchema.of("bob", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT)
        );

        var mapping = NodeLabelTokenToPropertyKeys.fixed(nodeSchema);

        assertThat(mapping.propertySchemas(NodeLabel.of("A"), importPropertySchemas)).isEqualTo(Map.of(
            "foo", PropertySchema.of("foo", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT),
            "bar", PropertySchema.of("bar", ValueType.DOUBLE, DefaultValue.forDouble(), PropertyState.PERSISTENT),
            "baz", PropertySchema.of("baz", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT)
        ));
        assertThat(mapping.propertySchemas(NodeLabel.of("B"), importPropertySchemas)).isEqualTo(Map.of(
            "baz", PropertySchema.of("baz", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT),
            "bob", PropertySchema.of("bob", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT)
        ));
        assertThat(mapping.propertySchemas(NodeLabel.of("C"), importPropertySchemas)).isEqualTo(Map.of());
    }

    @Test
    void shouldFailOnInconsistentPropertySchemas() {
        var nodeSchema = NodeSchema.empty()
            .addLabel(NodeLabel.of("A"), Map.of(
                "foo", PropertySchema.of("foo", ValueType.LONG)
            ));

        var fixed = NodeLabelTokenToPropertyKeys.fixed(nodeSchema);

        assertThatThrownBy(() -> fixed.propertySchemas(
            NodeLabel.of("A"),
            Map.of("bar", PropertySchema.of("bar", ValueType.DOUBLE))
        ))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Property schemas inferred from loading do not match input property schema.");
    }
}

