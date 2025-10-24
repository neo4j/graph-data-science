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
package org.neo4j.gds.procedures.algorithms.community.stream;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.procedures.algorithms.community.TriangleStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.triangle.TriangleResult;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TrianglesStreamTransformerTest {

    @Test
    void shouldTransformResult(){

       var result= Stream.of(new TriangleResult(0L,1L,2L));

        var closeableResources = mock(CloseableResourceRegistry.class);

        var transformer = new TrianglesStreamTransformer(closeableResources);

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new TriangleStreamResult(0L,1L,2L)
        );

        verify(closeableResources, times(1)).register(any());


    }


    @Test
    void shouldTransformEmptyResult(){

        var graphMock = mock(Graph.class);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));
        var closeableResources = mock(CloseableResourceRegistry.class);
        var transformer = new TrianglesStreamTransformer(closeableResources);

        var stream = transformer.apply(new TimedAlgorithmResult<>(Stream.of(),-1));
        assertThat(stream).isEmpty();


    }

}
