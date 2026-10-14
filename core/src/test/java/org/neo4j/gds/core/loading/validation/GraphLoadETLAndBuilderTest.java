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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;

import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class GraphLoadETLAndBuilderTest {
    @Test
    void shouldPickUpAndRunHooksInOrderBecauseWhyNot() {
        var graphStoreLoadedETL1 = mock(GraphStoreLoadedETL.class);
        var graphStoreLoadedETL2 = mock(GraphStoreLoadedETL.class);
        var graphStoreFilteredETL1 = mock(GraphStoreFilteredETL.class);
        var graphStoreFilteredETL2 = mock(GraphStoreFilteredETL.class);
        var graphLoadedETL1 = mock(GraphLoadedETL.class);
        var graphLoadedETL2 = mock(GraphLoadedETL.class);
        var graphLoadETL = GraphLoadETLBuilder.fromNothing()
            .withGraphStoreLoadedETL(graphStoreLoadedETL1)
            .withGraphStoreLoadedETL(graphStoreLoadedETL2)
            .withGraphStoreFilteredETL(graphStoreFilteredETL1)
            .withGraphStoreFilteredETL(graphStoreFilteredETL2)
            .withGraphLoadedETL(graphLoadedETL1)
            .withGraphLoadedETL(graphLoadedETL2)
            .build();

        var graphStore = mock(GraphStore.class);

        graphLoadETL.onGraphStoreLoaded(graphStore);
        verify(graphStoreLoadedETL1).onGraphStoreLoaded(graphStore);
        verify(graphStoreLoadedETL2).onGraphStoreLoaded(graphStore);

        var selectedLabels = Set.of(mock(NodeLabel.class));
        var selectedRelationshipTypes = Set.of(mock(RelationshipType.class));
        var relationshipProperty = Optional.of("weight property goes here");
        graphLoadETL.onGraphStoreFiltered(
            graphStore,
            selectedLabels,
            selectedRelationshipTypes,
            relationshipProperty
        );
        verify(graphStoreFilteredETL1).onGraphStoreFiltered(
            graphStore,
            selectedLabels,
            selectedRelationshipTypes,
            relationshipProperty
        );
        verify(graphStoreFilteredETL2).onGraphStoreFiltered(
            graphStore,
            selectedLabels,
            selectedRelationshipTypes,
            relationshipProperty
        );

        var graph = mock(Graph.class);
        graphLoadETL.onGraphLoaded(graph);
        verify(graphLoadedETL1).onGraphLoaded(graph);
        verify(graphLoadedETL2).onGraphLoaded(graph);
    }
}
