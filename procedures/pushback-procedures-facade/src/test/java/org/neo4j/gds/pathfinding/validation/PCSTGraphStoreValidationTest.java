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
package org.neo4j.gds.pathfinding.validation;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodeProperty;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PCSTGraphStoreValidationTest {

    @Test
    void shouldNotThrowForExistingProperty() {
        var graphStore = mock(GraphStore.class);
        var collectionString = Set.of("p");
        when(graphStore.nodePropertyKeys(anySet())).thenReturn(collectionString);

        var validation = new PCSTGraphStoreValidation("p");
        assertThatNoException().isThrownBy(() -> validation.validatePropertyExistence(
            graphStore,
            Set.of(NodeLabel.of("Node"))
        ));
    }

    @Test
    void shouldThrowForNonexistingProperty() {
        var graphStore = mock(GraphStore.class);
        var collectionString = Set.of("pNo");
        when(graphStore.nodePropertyKeys(anySet())).thenReturn(collectionString);

        var validation = new PCSTGraphStoreValidation("p");
        assertThatThrownBy(() -> validation.validatePropertyExistence(
            graphStore,
            Set.of(NodeLabel.of("Node"))
        )).hasMessageContaining(
            "Prize node property value type [p] not found in the graph."
        );
    }


    @Test
    void shouldNotThrowForValidPropertyType() {
        var graphStore = mock(GraphStore.class);
        var nodeProperty = mock(NodeProperty.class);
        when(nodeProperty.valueType()).thenReturn(ValueType.DOUBLE);
        when(graphStore.nodeProperty(anyString())).thenReturn(nodeProperty);

        var validation = new PCSTGraphStoreValidation("p");

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


        var validation = new PCSTGraphStoreValidation("p");

        assertThatThrownBy(() -> validation.validatePropertyType(
            graphStore
        ))
            .hasMessageContaining("Unsupported node property value type [FLOAT_ARRAY]. Value type required: [DOUBLE]");

    }

}
