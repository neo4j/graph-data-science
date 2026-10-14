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
package org.neo4j.gds.async;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.progress.tracking.ProgressTracker;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AsyncAlgorithmCallerTest {

    @Mock
    private AlgorithmCallable<String> algorithmCallableMock;

    @Mock
    private JobId jobIdMock;

    @Mock
    private Log logMock;

    @Test
    void algorithmComputesSuccessfully() {
        when(algorithmCallableMock.call()).thenReturn("Hello world!");

        var algorithmCaller = new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), logMock);

        var progressTrackerMock = mock(ProgressTracker.class);
        var future = algorithmCaller.run(algorithmCallableMock, jobIdMock, progressTrackerMock, true);

        await()
            .atMost(3, TimeUnit.SECONDS)
            .untilAsserted(() -> assertThat(future).isCompleted());

        verify(logMock, times(1)).debug("Job: `%s` - Starting", jobIdMock);
        verify(logMock, times(1)).debug("Job: `%s` - Complete", jobIdMock);
        verifyNoMoreInteractions(logMock);

        verify(progressTrackerMock).release();
        verifyNoMoreInteractions(progressTrackerMock);
    }

    @Test
    void algorithmComputesSuccessfullyWithoutRelease() {
        when(algorithmCallableMock.call()).thenReturn("Hello world!");

        var algorithmCaller = new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), logMock);

        var progressTrackerMock = mock(ProgressTracker.class);
        var future = algorithmCaller.run(algorithmCallableMock, jobIdMock, progressTrackerMock, false);

        await()
            .atMost(3, TimeUnit.SECONDS)
            .untilAsserted(() -> assertThat(future).isCompleted());

        verify(logMock, times(1)).debug("Job: `%s` - Starting", jobIdMock);
        verify(logMock, times(1)).debug("Job: `%s` - Complete", jobIdMock);
        verifyNoMoreInteractions(logMock);

        verifyNoMoreInteractions(progressTrackerMock);
    }

    @Test
    void algorithmComputesWithException() {
        var exception = new RuntimeException("Algorithm failed to compute.");
        when(algorithmCallableMock.call()).thenThrow(exception);

        var algorithmCaller = new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), logMock);

        var progressTrackerMock = mock(ProgressTracker.class);
        var future = algorithmCaller.run(algorithmCallableMock, jobIdMock, progressTrackerMock, true);

        await()
            .atMost(3, TimeUnit.SECONDS)
            .untilAsserted(() -> assertThat(future).isCompleted());

        assertThat(future.join()).isNull();

        verify(logMock, times(1)).debug("Job: `%s` - Starting", jobIdMock);
        verify(logMock, times(1)).error(
            org.mockito.ArgumentMatchers.eq("Job: `%s` - Failed"),
            org.mockito.ArgumentMatchers.eq(jobIdMock),
            org.mockito.ArgumentMatchers.argThat(t -> t instanceof java.util.concurrent.CompletionException && ((java.util.concurrent.CompletionException) t).getCause() == exception)
        );
        verify(logMock, times(1)).debug("Job: `%s` - Complete", jobIdMock);
        verifyNoMoreInteractions(logMock);

        verify(progressTrackerMock).endSubTaskWithFailure();
        verify(progressTrackerMock).release();
        verifyNoMoreInteractions(progressTrackerMock);
    }
}
