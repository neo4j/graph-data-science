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
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.services.GraphDimensionFactory;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.mem.MemoryTracker;
import org.neo4j.gds.mem.MemoryTree;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
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
    private GraphDimensionFactory graphDimensionFactoryMock;

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
        when(graphDimensionFactoryMock.create(any(Graph.class), any(GraphStore.class), anyCollection()))
            .thenReturn(GraphDimensions.of(23L, 87L));
        when(memoryEstimation.estimate(any(GraphDimensions.class), any(Concurrency.class)))
            .thenReturn(memoryTree);
    }

    @Test
    void shouldAllowExecution() {
        when(memoryTree.memoryUsage()).thenReturn(MemoryRange.of(13, 19));

        var memoryGuard = new DefaultMemoryGuard(
            logMock,
            graphDimensionFactoryMock,
            false,
            new MemoryTracker(42, logMock)
        );

        // there is enough memory available
        memoryGuard.assertAlgorithmCanRun(
            graphMock,
            graphStoreMock,
            Set.of(),
            CONCURRENCY,
            () -> memoryEstimation,
            new StandardLabel("some label"),
            DimensionTransformer.DISABLED,
            User.DEFAULT.getUsername(),
            jobIdMock,
            false
        );
    }

    @Test
    void shouldGuardExecutionUsingMinimumEstimate() {

        when(memoryTree.memoryUsage()).thenReturn(MemoryRange.of(117, 243));

        var memoryGuard = new DefaultMemoryGuard(
            logMock,
            graphDimensionFactoryMock,
            false,
            new MemoryTracker(42, logMock)
        );

        // uh oh
        assertThatIllegalStateException()
            .isThrownBy(
                () -> memoryGuard.assertAlgorithmCanRun(
                    graphMock,
                    graphStoreMock,
                    Set.of(),
                    CONCURRENCY,
                    () -> memoryEstimation,
                    new StandardLabel("some other label"),
                    DimensionTransformer.DISABLED,
                    User.DEFAULT.getUsername(),
                    jobIdMock,
                    false
                )
            )
            .withMessage("Memory required to run some other label (117b) exceeds available memory (42b)");
    }

    @Test
    void shouldGuardExecutionUsingMaximumEstimate() {

        when(memoryTree.memoryUsage()).thenReturn(MemoryRange.of(117, 243));

        var memoryGuard = new DefaultMemoryGuard(
            logMock,
            graphDimensionFactoryMock,
            true,
            new MemoryTracker(42, logMock)
        );

        // uh oh
        assertThatIllegalStateException()
            .isThrownBy(
                () -> memoryGuard.assertAlgorithmCanRun(
                    graphMock,
                    graphStoreMock,
                    Set.of(),
                    CONCURRENCY,
                    () -> memoryEstimation,
                    new StandardLabel("yet another label"),
                    DimensionTransformer.DISABLED,
                    User.DEFAULT.getUsername(),
                    jobIdMock,
                    false
                )
            )
            .withMessage("Memory required to run yet another label (243b) exceeds available memory (42b)");
    }

    @Test
    void shouldRespectSudoFlag() {

        when(memoryTree.memoryUsage()).thenReturn(MemoryRange.of(43, 99));

        var memoryGuard = new DefaultMemoryGuard(
            logMock,
            graphDimensionFactoryMock,
            false,
            new MemoryTracker(42, logMock)
        );

        assertThatIllegalStateException()
            .isThrownBy(
                () -> memoryGuard.assertAlgorithmCanRun(
                    graphMock,
                    graphStoreMock,
                    Set.of(),
                    CONCURRENCY,
                    () -> memoryEstimation,
                    new StandardLabel("some other label"),
                    DimensionTransformer.DISABLED,
                    User.DEFAULT.getUsername(),
                    jobIdMock,
                    false
                )
            );

        // now bypass the memory guard
        assertDoesNotThrow(() -> memoryGuard.assertAlgorithmCanRun(
            graphMock,
            graphStoreMock,
            Set.of(),
            CONCURRENCY,
            () -> memoryEstimation,
            new StandardLabel("some other label"),
            DimensionTransformer.DISABLED,
            User.DEFAULT.getUsername(),
            jobIdMock,
            true
        ));
    }
}
