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
package org.neo4j.gds.progress.tasks;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestTaskStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.progress.registration.TaskRegistryFactory;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.progress.tracking.TaskProgressTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

class TaskProgressTrackerFailMethodTest {

    @Test
    void failingTask() {
        var failingTask = Tasks.leaf("failingTask", new Concurrency(1));
        var log = new GdsTestLog();
        var taskStore = new TestTaskStore();

        var tracker = TaskProgressTracker.create(
            log,
            new LoggerForProgressTrackingAdapter(log),
            failingTask,
            new Concurrency(1),
            new JobId(),
            PlainSimpleRequestCorrelationId.create(),
            TaskRegistryFactory.local(taskStore, User.DEFAULT)
        );

        tracker.beginSubTask();
        tracker.endSubTaskWithFailure();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "failingTask :: Start",
                "failingTask 100%",
                "failingTask :: Failed"
            );

        assertThat(taskStore.queryRunning()).isEmpty();
        assertThat(taskStore.tasksSeen()).containsExactly("failingTask");
    }

    @Test
    void failingIntermediateTask() {

        var failingSubTask = Tasks.leaf("failingSubTask", new Concurrency(1));

        var rootTask = Tasks.task("rootTask", new Concurrency(1), failingSubTask);
        var log = new GdsTestLog();
        var taskStore = new TestTaskStore();

        var tracker = TaskProgressTracker.create(
            log,
            new LoggerForProgressTrackingAdapter(log),
            rootTask,
            new Concurrency(1),
            new JobId(),
            PlainSimpleRequestCorrelationId.create(),
            TaskRegistryFactory.local(taskStore, User.DEFAULT)
        );

        tracker.beginSubTask(/*rootTask*/);
        tracker.beginSubTask(/*failingSubTask*/);
        tracker.endSubTaskWithFailure();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "rootTask :: Start",
                "rootTask :: failingSubTask :: Start",
                "rootTask :: failingSubTask 100%",
                "rootTask :: failingSubTask :: Failed",
                "rootTask :: Failed"
            );

        assertThat(taskStore.queryRunning()).isEmpty();
        assertThat(taskStore.tasksSeen()).containsExactly("rootTask");
    }

}
