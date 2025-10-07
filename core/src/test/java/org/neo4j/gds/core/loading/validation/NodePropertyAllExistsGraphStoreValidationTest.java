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
import org.mockito.ArgumentMatchers;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NodePropertyAllExistsGraphStoreValidationTest {

    @Test
    void shouldNotThrowForExistingProperty() {
        var graphStore = mock(GraphStore.class);
        var collectionString = Set.of("p");
        when(graphStore.nodePropertyKeys(ArgumentMatchers.any(NodeLabel.class))).thenReturn(collectionString);
        doCallRealMethod().when(graphStore).nodePropertyKeys(anySet());

        var validation = new NodePropertyAllExistsGraphStoreValidation("p");
        assertThatNoException().isThrownBy(() -> validation.validatePropertyExists(
            graphStore,
            Set.of(NodeLabel.of("Node"))
        ));
    }

    @Test
    void shouldThrowForNonexistingPropertyInSomeLabels() {
        var graphStore = mock(GraphStore.class);
        var collectionString1 = Set.of("pNo");
        var collectionString2 = Set.of("p");

        var nodeLabel1 = NodeLabel.of("Node");
        var nodeLabel2 = NodeLabel.of("Node1");
        when(graphStore.nodePropertyKeys(ArgumentMatchers.eq(nodeLabel1))).thenReturn(collectionString1);
        when(graphStore.nodePropertyKeys(ArgumentMatchers.eq(nodeLabel2))).thenReturn(collectionString2);
        doCallRealMethod().when(graphStore).nodePropertyKeys(anySet());

        var validation = new NodePropertyAllExistsGraphStoreValidation("p");
        assertThatThrownBy(() -> validation.validatePropertyExists(
            graphStore,
            Set.of(nodeLabel1,nodeLabel2)
        )).hasMessageContaining(
            "Node property [p] not found in the graph"
        );
    }

    @Test
    void shouldThrowForNonexistingPropertyInEveryLabel() {
        var graphStore = mock(GraphStore.class);
        var collectionString = Set.of("pNo");

        var nodeLabel1 = NodeLabel.of("Node");
        var nodeLabel2 = NodeLabel.of("Node1");
        when(graphStore.nodePropertyKeys(ArgumentMatchers.eq(nodeLabel1))).thenReturn(collectionString);
        when(graphStore.nodePropertyKeys(ArgumentMatchers.eq(nodeLabel2))).thenReturn(collectionString);
        doCallRealMethod().when(graphStore).nodePropertyKeys(anySet());

        var validation = new NodePropertyAllExistsGraphStoreValidation("p");
        assertThatThrownBy(() -> validation.validatePropertyExists(
            graphStore,
            Set.of(nodeLabel1,nodeLabel2)
        )).hasMessageContaining(
            "Node property [p] not found in the graph"
        );
    }

}
