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
package org.neo4j.gds.mem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.UserTask;
import org.neo4j.gds.logging.Log;

import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MemoryTrackerTest {

    @ParameterizedTest
    @MethodSource("invalidInitialMemoryValues")
    void shouldNotAllowNegativeInitialValue(long initialMemoryValue) {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new MemoryTracker(initialMemoryValue, Log.noOpLog()))
            .withMessageContaining("Negative values are not allowed.");
    }

    @ParameterizedTest
    @MethodSource("validInitialMemoryValues")
    void shouldBeCreatedConsistently(long initialMemoryValue) {
        var memoryTracker = new MemoryTracker(initialMemoryValue, Log.noOpLog());

        assertThat(memoryTracker.initialMemory()).isEqualTo(initialMemoryValue);
    }

    @Test
    void shouldHaveAvailableMemorySameAsInitialBeforeAnyTracking() {
        var memoryTracker = new MemoryTracker(19L, Log.noOpLog());

        assertThat(memoryTracker.availableMemory())
            .isEqualTo(memoryTracker.availableMemory())
            .isEqualTo(19L);
    }

    @Test
    void shouldHaveAvailableMemoryWithoutTheTrackedMemory() {
        var memoryTracker = new MemoryTracker(19L, Log.noOpLog());

        memoryTracker.track("a","b",new JobId("foo"), 9);
        memoryTracker.track("a","b",new JobId("bar"), 3);

        assertThat(memoryTracker.availableMemory())
            .isEqualTo(memoryTracker.availableMemory())
            .isEqualTo(7L);
    }

    @Test
    void shouldFreeMemoryOnTaskRemoved() {
        var memoryTracker = new MemoryTracker(19L, Log.noOpLog());

        memoryTracker.track("a","b",new JobId("foo"), 9);
        memoryTracker.track("a","b",new JobId("bar"), 3);

        var userTaskMock = mock(UserTask.class, Answers.RETURNS_MOCKS);
        when(userTaskMock.jobId()).thenReturn(new JobId("foo"));

        memoryTracker.onTaskRemoved(userTaskMock);

        assertThat(memoryTracker.availableMemory())
            .isEqualTo(memoryTracker.availableMemory())
            .isEqualTo(16L);
    }

    static Stream<Arguments> validInitialMemoryValues() {
        return new Random()
            .longs(50, 0, Long.MAX_VALUE)
            .mapToObj(Arguments::of);
    }

    static Stream<Arguments> invalidInitialMemoryValues() {
        return new Random()
            .longs(50, Long.MIN_VALUE, 0)
            .mapToObj(Arguments::of);
    }

}
