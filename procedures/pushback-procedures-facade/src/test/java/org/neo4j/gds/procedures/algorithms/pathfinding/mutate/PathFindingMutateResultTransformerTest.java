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
package org.neo4j.gds.procedures.algorithms.pathfinding.mutate;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateRelationshipService;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.pathfinding.ShortestPathMutateStep;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class PathFindingMutateResultTransformerTest {

    @Test
    void shouldTransformToMutateResult() {
        var config = Map.<String, Object>of("foo", "bar");
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        var mutateStep = mock(ShortestPathMutateStep.class);

        var algoResult = mock(PathFindingResult.class);

        var relationshipsWritten = new RelationshipsWritten(5L);
        when(mutateStep.execute(any(), any(), any())).thenReturn(relationshipsWritten);

        var timedResult = new TimedAlgorithmResult<>(algoResult, 123L);

        var transformer = new PathFindingMutateResultTransformer(mutateStep, graph, graphStore, config);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isEqualTo(123L);
        assertThat(result.mutateMillis()).isNotNegative();
        assertThat(result.postProcessingMillis()).isZero();
        assertThat(result.configuration()).isEqualTo(config);
        assertThat(result.relationshipsWritten()).isEqualTo(5L);

        verify(mutateStep, times(1)).execute(graph, graphStore, algoResult);
        verifyNoMoreInteractions(mutateStep);
    }

    @Test
    void shouldTransformEmptyResultToMutateResult() {
        var config = Map.<String, Object>of("boo", "foo");
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        var mutateRelationshipService = new MutateRelationshipService(Log.noOpLog());
        var mutateStep = new ShortestPathMutateStep(
            "r",
            mutateRelationshipService
        );

        var bellmanFordResult = PathFindingResult.empty();

        var timedResult = new TimedAlgorithmResult<>(bellmanFordResult, 123L);

        var transformer = new PathFindingMutateResultTransformer(mutateStep, graph, graphStore, config);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isEqualTo(123L);
        assertThat(result.mutateMillis()).isNotNegative();
        assertThat(result.postProcessingMillis()).isZero();
        assertThat(result.configuration()).isEqualTo(config);
        assertThat(result.relationshipsWritten()).isEqualTo(0);

    }

}
