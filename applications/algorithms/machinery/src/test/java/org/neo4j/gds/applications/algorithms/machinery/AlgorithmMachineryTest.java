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
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class AlgorithmMachineryTest {

    @Test
    void shouldRunAlgorithm() {
        var algorithmMachinery = new AlgorithmMachinery();

        var progressTracker = mock(ProgressTracker.class);

        var algo = mock(Algorithm.class);
        when(algo.compute()).thenReturn("Hello, world!");

        var result = algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algo,
            progressTracker,
            false
        );

        assertThat(result).isEqualTo("Hello, world!");

        verifyNoInteractions(progressTracker);
    }

    @Test
    void shouldReleaseProgressTrackerWhenAsked() {
        var algorithmMachinery = new AlgorithmMachinery();

        var progressTracker = mock(ProgressTracker.class);

        var algo = mock(Algorithm.class);
        when(algo.compute()).thenReturn("Dodgers win world series!");

        var result = algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algo,
            progressTracker,
            true
        );

        assertThat(result).isEqualTo("Dodgers win world series!");

        verify(progressTracker).release();
    }

    @Test
    void shouldMarkProgressTracker() {
        var algorithmMachinery = new AlgorithmMachinery();

        var progressTracker = mock(ProgressTracker.class);
        var exception = new RuntimeException("Whoops!");

        var algo = mock(Algorithm.class);
        when(algo.compute()).thenThrow(exception);

        try {
            algorithmMachinery.runAlgorithmsAndManageProgressTracker(
                algo,
                progressTracker,
                false
            );
            fail();
        } catch (Exception e) {
            assertThat(e).hasMessage("Whoops!");
        }

        verify(progressTracker).endSubTaskWithFailure();
    }

    @Test
    void shouldMarkProgressTrackerAndReleaseIt() {
        var algorithmMachinery = new AlgorithmMachinery();

        var progressTracker = mock(ProgressTracker.class);
        var exception = new RuntimeException("Yeah, no...");

        var algo = mock(Algorithm.class);
        when(algo.compute()).thenThrow(exception);

        try {
            algorithmMachinery.runAlgorithmsAndManageProgressTracker(
                algo,
                progressTracker,
                true
            );
            fail();
        } catch (Exception e) {
            assertThat(e).hasMessage("Yeah, no...");
        }

        verify(progressTracker).endSubTaskWithFailure();
        verify(progressTracker).release();
    }
}
