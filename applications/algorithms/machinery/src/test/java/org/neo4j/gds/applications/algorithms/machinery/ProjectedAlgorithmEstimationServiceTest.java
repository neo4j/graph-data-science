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
package org.neo4j.gds.applications.algorithms.machinery;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.GraphParameters;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.mem.MemoryTree;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProjectedAlgorithmEstimationServiceTest {

    @Test
    void testEstimate() {

       var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
       var requestScopedDependencies = mock(RequestScopedDependencies.class);
       var service = new ProjectedAlgorithmEstimationService(graphStoreCatalogService, requestScopedDependencies);

        GraphName graphName = GraphName.parse("graph");
        GraphParameters graphParameters = mock(GraphParameters.class);
        MemoryEstimation algorithmEstimation = mock(MemoryEstimation.class);
        Concurrency concurrency = new Concurrency(4);
        DimensionTransformer transformer = DimensionTransformer.DISABLED;

        GraphResources graphResources = mock(GraphResources.class);
        when(graphStoreCatalogService.fetchGraphResources(any(), eq(graphName), any(), eq(graphParameters), any(), any(), eq(true), any()))
            .thenReturn(graphResources);


        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        when(graphResources.graph()).thenReturn(graph);
        when(graphResources.graphStore()).thenReturn(graphStore);
        when(graph.nodeCount()).thenReturn(100L);
        when(graph.relationshipCount()).thenReturn(200L);
        when(graphParameters.relationshipTypesFilter()).thenReturn(emptyList());

        // Mock algorithm estimation behavior
        MemoryTree algorithmTree = mock(MemoryTree.class);
        when(algorithmTree.render()).thenReturn("tree");
        when(algorithmTree.renderMap()).thenReturn(java.util.Map.of());
        when(algorithmTree.memoryUsage()).thenReturn(MemoryRange.of(1024, 2048));
        
        when(algorithmEstimation.estimate(any(), any())).thenReturn(algorithmTree);

        MemoryEstimateResult result = service.estimate(graphName, graphParameters, algorithmEstimation, concurrency, transformer);

        assertThat(result).isNotNull();
        assertThat(result.nodeCount()).isEqualTo(100);
        assertThat(result.relationshipCount()).isEqualTo(200);
        assertThat(result.bytesMin()).isEqualTo(1024);
        assertThat(result.bytesMax()).isEqualTo(2048);
    }
}
