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
package org.neo4j.gds.procedures.algorithms.pathfinding.write;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.pathfinding.KSpanningTreeWriteStep;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.spanningtree.SpanningTree;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class KSpanningTreeWriteResultTransformerTest {

    @Test
    void shouldTransformToWriteResult() {
        var config = Map.<String, Object>of("foo", "bar");
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        var resultStore = mock(ResultStore.class);
        var jobId = new JobId();
        var writeStep = mock(KSpanningTreeWriteStep.class);

        var algoResult = mock(SpanningTree.class);
        when(algoResult.effectiveNodeCount()).thenReturn(1L);
        when(algoResult.totalWeight()).thenReturn(3d);

        var timedResult = new TimedAlgorithmResult<>(algoResult, 123L);

        var transformer = new KSpanningTreeWriteResultTransformer(writeStep, graph, graphStore, resultStore, jobId, config);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isEqualTo(123L);
        assertThat(result.writeMillis()).isNotNegative();
        assertThat(result.configuration()).isEqualTo(config);
        assertThat(result.effectiveNodeCount()).isEqualTo(1L);

        verify(writeStep, times(1)).execute(graph, graphStore, resultStore, algoResult, jobId);
        verifyNoMoreInteractions(writeStep);
    }

}
