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
package org.neo4j.gds.core.loading.validation;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodeProperty;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NodePropertyTypeGraphStoreValidationTest {

    @Test
    void shouldNotThrowForValidPropertyType() {
        var graphStore = mock(GraphStore.class);
        var nodeProperty = mock(NodeProperty.class);
        when(nodeProperty.valueType()).thenReturn(ValueType.DOUBLE);
        when(graphStore.nodeProperty(anyString())).thenReturn(nodeProperty);

        var validation = new NodePropertyTypeRequirement("p", List.of(ValueType.DOUBLE));

        assertThatNoException().isThrownBy(() -> validation.validatePropertyType(
            graphStore
        ));


    }

    @Test
    void shouldThrowForInvalidType() {
        var graphStore = mock(GraphStore.class);
        var nodeProperty = mock(NodeProperty.class);
        when(nodeProperty.valueType()).thenReturn(ValueType.FLOAT_ARRAY);
        when(graphStore.nodeProperty(anyString())).thenReturn(nodeProperty);

        var validation = new NodePropertyTypeRequirement("p", List.of(ValueType.DOUBLE, ValueType.LONG));

        assertThatThrownBy(() -> validation.validatePropertyType(
            graphStore
        ))
            .hasMessageContaining("Unsupported node property value type for property `p`: FLOAT_ARRAY. Value types accepted: ['DOUBLE', 'LONG']");

    }

}
