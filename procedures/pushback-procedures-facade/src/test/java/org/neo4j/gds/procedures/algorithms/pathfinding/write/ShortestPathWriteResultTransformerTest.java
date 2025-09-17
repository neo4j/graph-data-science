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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.pathfinding.ShortestPathWriteStep;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.procedures.algorithms.results.StandardWriteRelationshipsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ShortestPathWriteResultTransformerTest {

    @Mock
    private ShortestPathWriteStep writeStepMock;
    @Mock
    private Graph graphMock;
    @Mock
    private GraphStore graphStoreMock;
    @Mock
    private ResultStore resultStoreMock;
    @Mock
    private JobId jobIdMock;
    @Mock
    private TimedAlgorithmResult<PathFindingResult> timedResultMock;

    @Test
    void shouldTransformResultCorrectly() {

        var pathFindingResult = mock(PathFindingResult.class);

        when(timedResultMock.result()).thenReturn(pathFindingResult);
        when(timedResultMock.computeMillis()).thenReturn(123L);

        var relationshipsWritten = new RelationshipsWritten(42);

        when(writeStepMock.execute(graphMock, graphStoreMock, resultStoreMock, pathFindingResult, jobIdMock))
                .thenReturn(relationshipsWritten);

        var configuration = Map.<String, Object>of("key", "value");
        var transformer = new ShortestPathWriteResultTransformer(
            writeStepMock,
            graphMock,
            graphStoreMock,
            resultStoreMock,
            jobIdMock,
            configuration
        );


        Stream<StandardWriteRelationshipsResult> resultStream = transformer.apply(timedResultMock);
        StandardWriteRelationshipsResult result = resultStream.findFirst().orElseThrow();

        assertThat(result.computeMillis()).isEqualTo(123L);
        assertThat(result.writeMillis()).isGreaterThanOrEqualTo(0L);
        assertThat(result.relationshipsWritten()).isEqualTo(42L);
        assertThat(result.configuration()).isEqualTo(configuration);
    }

}
