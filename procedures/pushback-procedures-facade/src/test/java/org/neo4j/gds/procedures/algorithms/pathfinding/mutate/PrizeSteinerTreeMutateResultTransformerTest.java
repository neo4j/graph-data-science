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
import org.neo4j.gds.pathfinding.PrizeCollectingSteinerTreeMutateStep;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class PrizeSteinerTreeMutateResultTransformerTest {

    @Test
    void shouldTransformToMutateResult() {
        var config = Map.<String, Object>of("foo", "bar");
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        var mutateStep = mock(PrizeCollectingSteinerTreeMutateStep.class);

        var algoResult = mock(PrizeSteinerTreeResult.class);
        when(algoResult.effectiveNodeCount()).thenReturn(1L);
        when(algoResult.sumOfPrizes()).thenReturn(2d);
        when(algoResult.totalWeight()).thenReturn(3d);


        var relationshipsWritten = new RelationshipsWritten(5L);
        when(mutateStep.execute(any(), any(), any())).thenReturn(relationshipsWritten);

        var timedResult = new TimedAlgorithmResult<>(algoResult, 123L);

        var transformer = new PrizeSteinerTreeMutateResultTransformer(mutateStep, graph, graphStore, config);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isEqualTo(123L);
        assertThat(result.mutateMillis()).isNotNegative();
        assertThat(result.configuration()).isEqualTo(config);
        assertThat(result.effectiveNodeCount()).isEqualTo(1L);
        assertThat(result.sumOfPrizes()).isEqualTo(2d);
        assertThat(result.totalWeight()).isEqualTo(3d);

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
        var mutateStep = new PrizeCollectingSteinerTreeMutateStep(
            "r",
            "foo",
            mutateRelationshipService
        );

        var algoResult = PrizeSteinerTreeResult.EMPTY;

        var timedResult = new TimedAlgorithmResult<>(algoResult, 123L);

        var transformer = new PrizeSteinerTreeMutateResultTransformer(mutateStep, graph, graphStore, config);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isEqualTo(123L);
        assertThat(result.mutateMillis()).isNotNegative();
        assertThat(result.configuration()).isEqualTo(config);
        assertThat(result.effectiveNodeCount()).isEqualTo(0L);
        assertThat(result.sumOfPrizes()).isEqualTo(0d);
        assertThat(result.totalWeight()).isEqualTo(0d);
        assertThat(result.relationshipsWritten()).isEqualTo(0);

    }

}
