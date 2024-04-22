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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.mem.MemoryTree;
import org.neo4j.gds.mem.MemoryGauge;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This is a saga of mocks. In my defense,
 * it is because the underlying classes aren't written in a tell-don't-ask manner.
 * And im not in the mood to change that right this minute.
 * So bear with and take it in the best spirit.
 */
class DefaultMemoryGuardTest {
    @Test
    void shouldAllowExecution() {
        var memoryGuard = new DefaultMemoryGuard(null, false, new MemoryGauge(new AtomicLong(42L)));

        var graph = mock(Graph.class);
        when(graph.nodeCount()).thenReturn(23L);
        when(graph.relationshipCount()).thenReturn(87L);
        var memoryEstimation = mock(MemoryEstimation.class);
        var memoryTree = mock(MemoryTree.class);
        when(memoryEstimation.estimate(GraphDimensions.of(23L, 87L), 7)).thenReturn(memoryTree);
        when(memoryTree.memoryUsage()).thenReturn(MemoryRange.of(13, 19));

        // there is enough memory available
        memoryGuard.assertAlgorithmCanRun(
            "Duckstra",
            new ExampleConfiguration(),
            graph,
            () -> memoryEstimation
        );
    }

    @Test
    void shouldGuardExecutionUsingMinimumEstimate() {
        var memoryGuard = new DefaultMemoryGuard(null, false, new MemoryGauge(new AtomicLong(42L)));

        var graph = mock(Graph.class);
        when(graph.nodeCount()).thenReturn(23L);
        when(graph.relationshipCount()).thenReturn(87L);
        var memoryEstimation = mock(MemoryEstimation.class);
        var memoryTree = mock(MemoryTree.class);
        when(memoryEstimation.estimate(GraphDimensions.of(23, 87), 7)).thenReturn(memoryTree);
        when(memoryTree.memoryUsage()).thenReturn(MemoryRange.of(117, 243));

        // uh oh
        try {
            memoryGuard.assertAlgorithmCanRun(
                "Duckstra",
                new ExampleConfiguration(),
                graph,
                () -> memoryEstimation
            );

            fail();
        } catch (IllegalStateException e) {
            assertThat(e).hasMessage("Memory required to run Duckstra (117b) exceeds available memory (42b)");
        }
    }

    @Test
    void shouldGuardExecutionUsingMaximumEstimate() {
        var memoryGuard = new DefaultMemoryGuard(null, true, new MemoryGauge(new AtomicLong(42L)));

        var graph = mock(Graph.class);
        when(graph.nodeCount()).thenReturn(23L);
        when(graph.relationshipCount()).thenReturn(87L);
        var memoryEstimation = mock(MemoryEstimation.class);
        var memoryTree = mock(MemoryTree.class);
        when(memoryEstimation.estimate(GraphDimensions.of(23, 87), 7)).thenReturn(memoryTree);
        when(memoryTree.memoryUsage()).thenReturn(MemoryRange.of(117, 243));

        // uh oh
        try {
            memoryGuard.assertAlgorithmCanRun(
                "Duckstra",
                new ExampleConfiguration(),
                graph,
                () -> memoryEstimation
            );

            fail();
        } catch (IllegalStateException e) {
            assertThat(e).hasMessage("Memory required to run Duckstra (243b) exceeds available memory (42b)");
        }
    }
}
