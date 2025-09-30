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
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.pathfinding.MaxFlowWriteStep;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class MaxFlowWriteResultTransformerTest {

    @Test
    void shouldTransformToWriteResult() {
        var config = Map.<String, Object>of("foo", "bar");
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        var resultStore = mock(ResultStore.class);
        var jobId = new JobId();
        var writeStep = mock(MaxFlowWriteStep.class);

        var algoResult = mock(FlowResult.class);
        when(algoResult.totalFlow()).thenReturn(3D);

        var relationshipsWritten = new RelationshipsWritten(5L);
        when(writeStep.execute(any(), any(), any(), any(), any())).thenReturn(relationshipsWritten);

        var timedResult = new TimedAlgorithmResult<>(algoResult, 123L);

        var transformer = new MaxFlowWriteResultTransformer(writeStep, graph, graphStore, resultStore, jobId, config);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isEqualTo(123L);
        assertThat(result.writeMillis()).isNotNegative();
        assertThat(result.configuration()).isEqualTo(config);
        assertThat(result.totalFlow()).isEqualTo(3D);

        assertThat(result.relationshipsWritten()).isEqualTo(5L);

        verify(writeStep, times(1)).execute(graph, graphStore, resultStore, algoResult, jobId);
        verifyNoMoreInteractions(writeStep);
    }

    @Test
    void shouldTransformEmptyResultToWriteResult() {
        var config = Map.<String, Object>of("boo", "foo");
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        var resultStore = mock(ResultStore.class);
        var jobId = new JobId();
        var writeStep = mock(MaxFlowWriteStep.class);
        when(writeStep.execute(any(), any(), any(), any(), any())).thenReturn(new RelationshipsWritten(0L));

        var algoResult = FlowResult.EMPTY;

        var timedResult = new TimedAlgorithmResult<>(algoResult, 123L);

        var transformer = new MaxFlowWriteResultTransformer(writeStep, graph, graphStore, resultStore, jobId, config);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isEqualTo(123L);
        assertThat(result.writeMillis()).isNotNegative();
        assertThat(result.configuration()).isEqualTo(config);
        assertThat(result.totalFlow()).isEqualTo(0d);
        assertThat(result.relationshipsWritten()).isEqualTo(0);
    }
}
