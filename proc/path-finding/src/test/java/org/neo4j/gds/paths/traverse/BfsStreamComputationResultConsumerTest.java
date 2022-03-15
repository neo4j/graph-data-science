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
package org.neo4j.gds.paths.traverse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BfsStreamComputationResultConsumerTest {

    @Mock ComputationResult<BFS, HugeLongArray, BfsStreamConfig> computationResultMock;
    @Mock ExecutionContext executionContextMock;
    @Mock ProcedureCallContext procedureCallContextMock;
    @Mock BfsStreamConfig configMock;
    @Mock Graph graphMock;

    @BeforeEach
    void setUp() {
        when(graphMock.isEmpty()).thenReturn(false);
        doReturn(1L, 2L).when(graphMock).toOriginalNodeId(anyLong());

        when(computationResultMock.graph()).thenReturn(graphMock);

        var result = HugeLongArray.newArray(2);
        when(computationResultMock.result()).thenReturn(result);

        when(configMock.sourceNode()).thenReturn(0L);
        when(computationResultMock.config()).thenReturn(configMock);

        when(executionContextMock.callContext()).thenReturn(procedureCallContextMock);
    }

    @Test
    void shouldNotComputePath() {
        when(procedureCallContextMock.outputFields()).thenReturn(Stream.of("sourceNode", "nodeIds"));

        var consumer = new BfsStreamComputationResultConsumer();

        var actual = consumer.consume(computationResultMock, executionContextMock);
        assertThat(actual)
            .hasSize(1)
            .containsExactly(
                new BfsStreamResult(0, List.of(1L, 2L), null)
            );
    }

    @Test
    void shouldComputePath() {
        var kernelTransactionMock = mock(KernelTransaction.class);
        var internalTransactionMock = mock(InternalTransaction.class);
        doReturn(mock(Node.class)).when(internalTransactionMock).getNodeById(anyLong());
        when(kernelTransactionMock.internalTransaction()).thenReturn(internalTransactionMock);
        when(executionContextMock.transaction()).thenReturn(kernelTransactionMock);
        when(procedureCallContextMock.outputFields()).thenReturn(Stream.of("sourceNode", "nodeIds", "path"));

        var consumer = new BfsStreamComputationResultConsumer();

        var actual = consumer.consume(computationResultMock, executionContextMock).collect(Collectors.toList());
        assertThat(actual)
            .hasSize(1)
            .satisfiesExactly(resultRow -> assertThat(resultRow.path).isNotNull());
    }

}
