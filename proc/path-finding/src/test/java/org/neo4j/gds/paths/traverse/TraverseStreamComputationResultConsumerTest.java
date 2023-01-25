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

import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class TraverseStreamComputationResultConsumerTest {

    @Test
    void shouldNotComputePath() {
        var pathFactoryFacadeMock = mock(PathFactoryFacade.class);
        var result = TraverseStreamComputationResultConsumer.consume(
            0L,
            HugeLongArray.of(1L, 2L),
            l -> l,
            false,
            TestResult::new,
            false,
            pathFactoryFacadeMock,
            RelationshipType.withName("TEST"),
            mock(InternalTransaction.class)::getNodeById
        );

        verifyNoInteractions(pathFactoryFacadeMock);

        assertThat(result)
            .hasSize(1)
            .allSatisfy(r -> {
                assertThat(r.path).isNull();
                assertThat(r.sourceNode).isEqualTo(0);
                assertThat(r.nodeIds).containsExactly(1L, 2L);
            });
    }

    @Test
    void shouldComputePath() {
        var pathFactoryFacadeMock = mock(PathFactoryFacade.class);
        doReturn(mock(Path.class)).when(pathFactoryFacadeMock).createPath(any(), any(), any());
        var result = TraverseStreamComputationResultConsumer.consume(
            0L,
            HugeLongArray.of(1L, 2L),
            l -> l,
            false,
            TestResult::new,
            true,
            pathFactoryFacadeMock,
            RelationshipType.withName("TEST"),
            mock(InternalTransaction.class)::getNodeById
        );

        assertThat(result)
            .hasSize(1)
            .allSatisfy(r -> {
                assertThat(r.path).isNotNull();
                assertThat(r.sourceNode).isEqualTo(0);
                assertThat(r.nodeIds).containsExactly(1L, 2L);
            });
    }

    private static class TestResult {
        public final Long sourceNode;
        public final List<Long> nodeIds;
        public final Path path;

        TestResult(long sourceNode, @Nullable List<Long> nodes, @Nullable Path path) {
            this.sourceNode = sourceNode;
            this.nodeIds = nodes;
            this.path = path;
        }
    }
}
