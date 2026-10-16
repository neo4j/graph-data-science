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
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.memest.GraphMemoryEstimation;
import org.neo4j.gds.memest.GraphStoreEstimationService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UnprojectedAlgorithmEstimationServiceTest {

    @Test
    void testEstimate() {

        var requestScopedDependencies = mock(RequestScopedDependencies.class);
        when(requestScopedDependencies.correlationId()).thenReturn(mock(RequestCorrelationId.class));

        var graphStoreEstimationService = mock(GraphStoreEstimationService.class);

        var dimensions =  ImmutableGraphDimensions.builder().nodeCount(100).relCountUpperBound(200).build();

        var graphEstimation =  MemoryEstimations.builder("Graph Estimation").fixed("a",1000).build();

        var graphMemoryEstimation = mock(GraphMemoryEstimation.class);
        when(graphMemoryEstimation.estimateMemoryUsageAfterLoading()).thenReturn(graphEstimation);
        when(graphMemoryEstimation.dimensions()).thenReturn(dimensions);

        when(graphStoreEstimationService.estimate(any(),any())).thenReturn(graphMemoryEstimation);

        var service = new UnprojectedAlgorithmEstimationService(graphStoreEstimationService, requestScopedDependencies);

        GraphProjectConfig config = mock(GraphProjectConfig.class);
        Concurrency concurrency = new Concurrency(4);
        DimensionTransformer transformer = DimensionTransformer.DISABLED;

        var algoEstimation =  MemoryEstimations.builder("Algo Estimation").perNode("a",(nodeCount) -> nodeCount).build();

        MemoryEstimateResult result = service.estimate(config, algoEstimation, concurrency, transformer);

        assertThat(result).isNotNull();
        assertThat(result.nodeCount()).isEqualTo(100);
        assertThat(result.relationshipCount()).isEqualTo(200);
        assertThat(result.bytesMin()).isEqualTo(1100);
        assertThat(result.bytesMax()).isEqualTo(1100);

    }
}
