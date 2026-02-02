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
package org.neo4j.gds.procedures.algorithms.centrality.stream;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.beta.pregel.NodeValue;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.procedures.algorithms.centrality.HitsStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class HitsStreamResultTransformerTest {

    @Test
    void shouldTransformResult(){

        var auth = HugeDoubleArray.of(10, 9, 8);
        var hub = HugeDoubleArray.of(7, 6, 5);
        var nodeValues = mock(NodeValue.class);
        when(nodeValues.doubleProperties(eq("auth"))).thenReturn(auth);
        when(nodeValues.doubleProperties(eq("hub"))).thenReturn(hub);
        var result = mock(PregelResult.class);
        when(result.nodeValues()).thenReturn(nodeValues);

        var idMap = mock(IdMap.class);
        when(idMap.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new HitsStreamResultTransformer("auth","hub",idMap);

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new HitsStreamResult(0, Map.of("auth",10D,"hub",7D)),
            new HitsStreamResult(1, Map.of("auth",9D,"hub",6D)),
            new HitsStreamResult(2, Map.of("auth",8D,"hub",5D))
        );

        verify(idMap, times(1)).toOriginalNodeId(0L);
        verify(idMap, times(1)).toOriginalNodeId(1L);
        verify(idMap, times(1)).toOriginalNodeId(2L);
        verifyNoMoreInteractions(idMap);

    }

}
