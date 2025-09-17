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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.pathfinding.BellmanFordWriteStep;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.BellmanFordWriteResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BellmanFordWriteResultTransformerTest {

    @Mock
    private BellmanFordWriteStep bellmanFordWriteStepMock;
    @Mock
    private Graph graphMock;
    @Mock
    private GraphStore graphStoreMock;
    @Mock
    private ResultStore resultStoreMock;
    @Mock
    private JobId jobIdMock;
    @Mock
    private TimedAlgorithmResult<BellmanFordResult> timedResultMock;

    private Map<String, Object> configuration;
    private BellmanFordWriteResultTransformer transformer;

    @BeforeEach
    void setUp() {
        configuration = Map.of("key", "value");
        transformer = new BellmanFordWriteResultTransformer(
            bellmanFordWriteStepMock,
            graphMock,
            graphStoreMock,
            resultStoreMock,
            jobIdMock,
            configuration
        );
    }

    @Test
    void shouldTransformResultCorrectly() {
        var bellmanFordResult = mock(BellmanFordResult.class);
        when(bellmanFordResult.containsNegativeCycle()).thenReturn(false);

        when(timedResultMock.result()).thenReturn(bellmanFordResult);
        when(timedResultMock.computeMillis()).thenReturn(123L);

        var relationshipsWritten = new RelationshipsWritten(42);

        when(bellmanFordWriteStepMock.execute(graphMock, graphStoreMock, resultStoreMock, bellmanFordResult, jobIdMock))
                .thenReturn(relationshipsWritten);

        Stream<BellmanFordWriteResult> resultStream = transformer.apply(timedResultMock);
        BellmanFordWriteResult result = resultStream.findFirst().orElseThrow();

        assertThat(result.computeMillis()).isEqualTo(123L);
        assertThat(result.writeMillis()).isGreaterThanOrEqualTo(0L);
        assertThat(result.relationshipsWritten()).isEqualTo(42L);
        assertThat(result.containsNegativeCycle()).isFalse();
        assertThat(result.configuration()).isEqualTo(configuration);
    }

    @Test
    void shouldHandleNegativeCycle() {
        var bellmanFordResult = mock(BellmanFordResult.class);
        when(bellmanFordResult.containsNegativeCycle()).thenReturn(true);

        when(timedResultMock.result()).thenReturn(bellmanFordResult);
        when(timedResultMock.computeMillis()).thenReturn(0L);

        var relationshipsWritten = new RelationshipsWritten(0);

        when(bellmanFordWriteStepMock.execute(graphMock, graphStoreMock, resultStoreMock, bellmanFordResult, jobIdMock))                .thenReturn(relationshipsWritten);

        var resultStream = transformer.apply(timedResultMock);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.containsNegativeCycle()).isTrue();
        assertThat(result.relationshipsWritten()).isZero();
    }

}
