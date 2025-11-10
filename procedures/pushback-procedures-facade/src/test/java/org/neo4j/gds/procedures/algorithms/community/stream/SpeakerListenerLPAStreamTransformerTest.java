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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.pregel.ImmutablePregelResult;
import org.neo4j.gds.beta.pregel.NodeValue;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.procedures.algorithms.community.SpeakerListenerLPAStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class SpeakerListenerLPAStreamTransformerTest {

    @Test
    void shouldTransformResult(){

        var schema= NodeValue.of(new PregelSchema.Builder().add("communityIds", ValueType.LONG_ARRAY).build(),2,new Concurrency(1));
        long[] value1 = {1, 2};
        schema.set("communityIds",0, value1);
        long[] value2 = {2, 1};
        schema.set("communityIds",1, value2);

        var result= ImmutablePregelResult.of(schema, 2, false);

        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(2L);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new SpeakerListenerLPAStreamTransformer(graphMock);

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new SpeakerListenerLPAStreamResult(0, Map.of("communityIds",value1)),
            new SpeakerListenerLPAStreamResult(1, Map.of("communityIds",value2))
        );

        verify(graphMock, times(1)).toOriginalNodeId(0L);
        verify(graphMock, times(1)).toOriginalNodeId(1L);
        verify(graphMock, times(1)).nodeCount();

        verifyNoMoreInteractions(graphMock);

    }


    @Test
    void shouldTransformEmptyResult(){

        var graphMock = mock(Graph.class);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));
        var empty= NodeValue.of(new PregelSchema.Builder().build(),0,new Concurrency(1));
        var emptyResult= ImmutablePregelResult.of(empty, 0, false);

        var transformer = new SpeakerListenerLPAStreamTransformer(graphMock);

        var stream = transformer.apply(new TimedAlgorithmResult<>(emptyResult,-1));
        assertThat(stream).isEmpty();

        verifyNoMoreInteractions(graphMock);

    }

}
