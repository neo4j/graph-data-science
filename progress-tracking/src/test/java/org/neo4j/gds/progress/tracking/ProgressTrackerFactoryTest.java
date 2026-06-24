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
package org.neo4j.gds.progress.tracking;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.progress.registration.TaskRegistryFactory;
import org.neo4j.gds.progress.logging.LoggerForProgressTracking;
import org.neo4j.gds.progress.tasks.Progress;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.logging.Log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProgressTrackerFactoryTest {
    @Mock
    private Log log;
    @Mock
    private LoggerForProgressTracking loggerForProgressTracking;
    @Mock
    private RequestCorrelationId requestCorrelationId;
    @Mock
    private TaskRegistryFactory taskRegistryFactory;
    @Mock
    private Task task;
    @Mock
    private Progress progress;

    @Test
    void shouldCreateTaskProgressTracker() {
        var concurrency = new Concurrency(2);
        when(progress.volume()).thenReturn(10L);
        when(task.getProgress()).thenReturn(progress);

        var factory = new ProgressTrackerFactory(
            log,
            loggerForProgressTracking,
            requestCorrelationId,
            taskRegistryFactory
        );
        var tracker = factory.create(task, new JobId("jid-test"), concurrency, true);
        assertThat(tracker).isInstanceOf(TaskProgressTracker.class);
    }

    @Test
    void shouldCreateTaskTreeProgressTracker() {
        var concurrency = new Concurrency(4);
        when(progress.volume()).thenReturn(1L);
        when(task.getProgress()).thenReturn(progress);

        var factory = new ProgressTrackerFactory(
            log,
            loggerForProgressTracking,
            requestCorrelationId,
            taskRegistryFactory
        );

        var tracker = factory.create(task, new JobId("jid-test"), concurrency, false);
        assertThat(tracker).isInstanceOf(TaskTreeProgressTracker.class);
    }

}
