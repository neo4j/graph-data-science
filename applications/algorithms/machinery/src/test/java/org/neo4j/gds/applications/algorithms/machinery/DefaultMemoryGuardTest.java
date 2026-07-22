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
package org.neo4j.gds.applications.algorithms.machinery;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.mem.MemoryTree;
import org.neo4j.gds.memory.tracking.AvailableMemoryReservationExceededException;
import org.neo4j.gds.memory.tracking.MemoryGuardException;
import org.neo4j.gds.memory.tracking.MemoryTracker;
import org.neo4j.gds.memory.tracking.TotalMemoryReservationExceededException;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This is a saga of mocks. In my defense,
 * it is because the underlying classes aren't written in a tell-don't-ask manner.
 * And im not in the mood to change that right this minute.
 * So bear with and take it in the best spirit.
 */
@ExtendWith(MockitoExtension.class)
class DefaultMemoryGuardTest {

    private static final Concurrency CONCURRENCY = new Concurrency(4);

    @Mock
    private GraphDimensionsFactory graphDimensionFactoryMock;

    @Mock
    private Log logMock;

    @Mock
    private Graph graphMock;

    @Mock
    private GraphStore graphStoreMock;

    @Mock
    private JobId jobIdMock;

    @Mock
    private MemoryEstimation memoryEstimation;

    @Mock
    private MemoryTree memoryTree;

    @BeforeEach
    void setUp() {
        when(graphDimensionFactoryMock.graphDimensions(any(GraphStore.class), any(Graph.class),anyCollection()))
            .thenReturn(GraphDimensions.of(23L, 87L));
        when(memoryEstimation.estimate(any(GraphDimensions.class), any(Concurrency.class)))
            .thenReturn(memoryTree);
    }

    @Test
    void shouldAllowExecution() throws MemoryGuardException {
        when(memoryTree.memoryUsage()).thenReturn(MemoryRange.of(13, 19));

        var memoryTracker = mock(MemoryTracker.class);
        var memoryGuard = new DefaultMemoryGuard(
            logMock,
            graphDimensionFactoryMock,
            false,
            memoryTracker
        );

        doNothing().when(memoryTracker).tryToTrack("Mark", "labels everywhere", jobIdMock, 13L);
        memoryGuard.assertAlgorithmCanRun(
            graphMock,
            graphStoreMock,
            Set.of(),
            CONCURRENCY,
            () -> memoryEstimation,
            new StandardLabel("labels everywhere"),
            DimensionTransformer.DISABLED,
            "Mark",
            jobIdMock,
            false
        );
    }

    @Test
    void shouldGuardExecutionUsingMinimumEstimate() throws MemoryGuardException {

        when(memoryTree.memoryUsage()).thenReturn(MemoryRange.of(117, 243));

        var memoryTracker = mock(MemoryTracker.class);
        var memoryGuard = new DefaultMemoryGuard(
            logMock,
            graphDimensionFactoryMock,
            false,
            memoryTracker
        );

        var memoryGuardException = new TotalMemoryReservationExceededException("foo", 7, 5);
        doThrow(memoryGuardException).when(memoryTracker).tryToTrack(
            "Alice",
            "some other label",
            jobIdMock,
            117L
        );
        assertThatThrownBy(
            () -> memoryGuard.assertAlgorithmCanRun(
                    graphMock,
                    graphStoreMock,
                    Set.of(),
                    CONCURRENCY,
                    () -> memoryEstimation,
                    new StandardLabel("some other label"),
                    DimensionTransformer.DISABLED,
                    "Alice",
                    jobIdMock,
                    false
                )
            )
            .isSameAs(memoryGuardException);
    }

    @Test
    void shouldGuardExecutionUsingMaximumEstimate() throws MemoryGuardException {

        when(memoryTree.memoryUsage()).thenReturn(MemoryRange.of(117, 243));

        var memoryTracker = mock(MemoryTracker.class);
        var memoryGuard = new DefaultMemoryGuard(
            logMock,
            graphDimensionFactoryMock,
            true,
            memoryTracker
        );

        var memoryGuardException = new AvailableMemoryReservationExceededException("bar", 19, 15);
        doThrow(memoryGuardException).when(memoryTracker).tryToTrack(
            "Bob",
            "yet another label",
            jobIdMock,
            243L
        );
        assertThatThrownBy(
            ()  -> memoryGuard.assertAlgorithmCanRun(
                    graphMock,
                    graphStoreMock,
                    Set.of(),
                    CONCURRENCY,
                    () -> memoryEstimation,
                    new StandardLabel("yet another label"),
                    DimensionTransformer.DISABLED,
                    "Bob",
                    jobIdMock,
                    false
                )
            )
            .isSameAs(memoryGuardException);
    }

    @Test
    void shouldRespectSudoFlag() {

        when(memoryTree.memoryUsage()).thenReturn(MemoryRange.of(43, 99));

        var memoryTracker = mock(MemoryTracker.class);
        var memoryGuard = new DefaultMemoryGuard(
            logMock,
            graphDimensionFactoryMock,
            false,
            memoryTracker
        );

        assertDoesNotThrow(() -> memoryGuard.assertAlgorithmCanRun(
            graphMock,
            graphStoreMock,
            Set.of(),
            CONCURRENCY,
            () -> memoryEstimation,
            new StandardLabel("labels galore"),
            DimensionTransformer.DISABLED,
            "Eve",
            jobIdMock,
            true
        ));

        verify(memoryTracker).track("Eve", "labels galore", jobIdMock, 43L);
    }
}
