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
package org.neo4j.gds.applications.algorithms.similarity;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.similarity.knn.KnnNodePropertySpec;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KnnHookTest {

    @Test
    void shouldNotThrowForExistingProperty(){
        var  graph = mock(Graph.class);
        var nodeProp = mock(LongNodePropertyValues.class);
        when(graph.nodeProperties("foo")).thenReturn(nodeProp);

        var knnSpec = mock(KnnNodePropertySpec.class);
        when(knnSpec.name()).thenReturn("foo");

        var hook = new KnnHook(List.of(knnSpec));

        assertDoesNotThrow( () -> hook.onGraphLoaded(graph));
    }

    @Test
    void shouldThrowForNonExistingProperty(){
        var  graph = mock(Graph.class);
        var nodeProp = mock(LongNodePropertyValues.class);
        when(graph.nodeProperties("foo")).thenReturn(nodeProp);
        when(graph.nodeProperties("bar")).thenReturn(null);
        when(graph.availableNodeProperties()).thenReturn(Set.of("foo"));

        var knnSpec1 = mock(KnnNodePropertySpec.class);
        when(knnSpec1.name()).thenReturn("foo");
        var knnSpec2 = mock(KnnNodePropertySpec.class);
        when(knnSpec2.name()).thenReturn("bar");

        var hook = new KnnHook(List.of(knnSpec1, knnSpec2));

        assertThatThrownBy(() -> hook.onGraphLoaded(graph))
            .hasMessageContaining("The property `bar` has not been loaded. Available properties: ['foo']");
    }

}
