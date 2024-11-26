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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AlgorithmMachineryTest {

    private static final Concurrency CONCURRENCY = new Concurrency(4);
    @Mock
    private Algorithm<String> algo;

    @Test
    void shouldRunAlgorithm() {
        var algorithmMachinery = new AlgorithmMachinery();

        var progressTracker = mock(ProgressTracker.class);

        when(algo.compute()).thenReturn("Hello, world!");

        var result = algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algo,
            progressTracker,
            false,
            CONCURRENCY
        );

        assertThat(result).isEqualTo("Hello, world!");

        verify(progressTracker, times(1)).requestedConcurrency(CONCURRENCY);
        verifyNoMoreInteractions(progressTracker);
    }

    @Test
    void shouldReleaseProgressTrackerWhenAsked() {
        var algorithmMachinery = new AlgorithmMachinery();

        var progressTracker = mock(ProgressTracker.class);

        when(algo.compute()).thenReturn("Dodgers win world series!");

        var result = algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algo,
            progressTracker,
            true,
            CONCURRENCY
        );

        assertThat(result).isEqualTo("Dodgers win world series!");

        verify(progressTracker, times(1)).requestedConcurrency(CONCURRENCY);
        verify(progressTracker, times(1)).release();
        verifyNoMoreInteractions(progressTracker);
    }

    @Test
    void shouldMarkProgressTracker() {
        var algorithmMachinery = new AlgorithmMachinery();

        var progressTracker = mock(ProgressTracker.class);
        var exception = new RuntimeException("Whoops!");

        when(algo.compute()).thenThrow(exception);

        try {
            algorithmMachinery.runAlgorithmsAndManageProgressTracker(
                algo,
                progressTracker,
                false,
                CONCURRENCY
            );
            fail();
        } catch (Exception e) {
            assertThat(e).hasMessage("Whoops!");
        }

        verify(progressTracker, times(1)).requestedConcurrency(CONCURRENCY);
        verify(progressTracker, times(1)).endSubTaskWithFailure();
        verifyNoMoreInteractions(progressTracker);
    }

    @Test
    void shouldMarkProgressTrackerAndReleaseIt() {
        var algorithmMachinery = new AlgorithmMachinery();

        var progressTracker = mock(ProgressTracker.class);
        var exception = new RuntimeException("Yeah, no...");

        when(algo.compute()).thenThrow(exception);

        try {
            algorithmMachinery.runAlgorithmsAndManageProgressTracker(
                algo,
                progressTracker,
                true,
                CONCURRENCY
            );
            fail();
        } catch (Exception e) {
            assertThat(e).hasMessage("Yeah, no...");
        }

        verify(progressTracker, times(1)).requestedConcurrency(CONCURRENCY);
        verify(progressTracker, times(1)).endSubTaskWithFailure();
        verify(progressTracker, times(1)).release();
        verifyNoMoreInteractions(progressTracker);
    }
}
