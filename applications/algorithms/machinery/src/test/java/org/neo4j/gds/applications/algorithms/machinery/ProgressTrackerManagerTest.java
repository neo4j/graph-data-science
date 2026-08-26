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
import org.neo4j.gds.progress.tracking.ProgressTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProgressTrackerManagerTest {
    @Mock
    private Algorithm<String> algo;

    @Test
    void shouldGetResult() {
        var progressTrackerManager = new ProgressTrackerManager();

        var progressTracker = mock(ProgressTracker.class);

        when(algo.compute()).thenReturn("Hello, world!");

        var result = progressTrackerManager.runAlgorithmAndManageProgressTracker(
            algo,
            progressTracker,
            false
        );

        assertThat(result).isEqualTo("Hello, world!");

        verifyNoMoreInteractions(progressTracker);
    }

    @Test
    void shouldReleaseProgressTrackerWhenAsked() {
        var progressTrackerManager = new ProgressTrackerManager();

        var progressTracker = mock(ProgressTracker.class);

        when(algo.compute()).thenReturn("Dodgers win world series!");

        var result = progressTrackerManager.runAlgorithmAndManageProgressTracker(
            algo,
            progressTracker,
            true
        );

        assertThat(result).isEqualTo("Dodgers win world series!");

        verify(progressTracker, times(1)).release();
        verifyNoMoreInteractions(progressTracker);
    }

    @Test
    void shouldMarkProgressTracker() {
        var progressTrackerManager = new ProgressTrackerManager();

        var progressTracker = mock(ProgressTracker.class);
        var exception = new RuntimeException("Whoops!");

        when(algo.compute()).thenThrow(exception);

        try {
            progressTrackerManager.runAlgorithmAndManageProgressTracker(
                algo,
                progressTracker,
                false
            );
            fail();
        } catch (Exception e) {
            assertThat(e).hasMessage("Whoops!");
        }

        verify(progressTracker, times(1)).endSubTaskWithFailure();
        verifyNoMoreInteractions(progressTracker);
    }

    @Test
    void shouldMarkProgressTrackerAndReleaseIt() {
        var progressTrackerManager = new ProgressTrackerManager();

        var progressTracker = mock(ProgressTracker.class);
        var exception = new RuntimeException("Yeah, no...");

        when(algo.compute()).thenThrow(exception);

        try {
            progressTrackerManager.runAlgorithmAndManageProgressTracker(
                algo,
                progressTracker,
                true
            );
            fail();
        } catch (Exception e) {
            assertThat(e).hasMessage("Yeah, no...");
        }

        verify(progressTracker, times(1)).endSubTaskWithFailure();
        verify(progressTracker, times(1)).release();
        verifyNoMoreInteractions(progressTracker);
    }
}
