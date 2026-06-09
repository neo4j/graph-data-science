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
package org.neo4j.gds.core.utils.progress.tasks;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.RequestCorrelationIdForTesting;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.RenamesCurrentThread;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.PerDatabaseTaskStore;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.UserTask;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.logging.Log;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.WARN;

class TaskProgressTrackerTest {

    @Test
    void shouldStepThroughSubtasks() {
        var leafTask = Tasks.leaf("leaf1", new Concurrency(1));
        var iterativeTask = Tasks.iterativeFixed("iterative",
            new Concurrency(1),
            () -> List.of(Tasks.leaf("leaf2", new Concurrency(1))), 2);
        var rootTask = Tasks.task(
            "root",
            new Concurrency(1), leafTask,
            iterativeTask
        );

        var progressTracker = progressTracker(rootTask);

        progressTracker.beginSubTask();
        assertThat(progressTracker.currentSubTask()).isEqualTo(rootTask);
        assertThat(rootTask.status()).isEqualTo(Status.RUNNING);

        progressTracker.beginSubTask();
        assertThat(progressTracker.currentSubTask()).isEqualTo(leafTask);
        assertThat(leafTask.status()).isEqualTo(Status.RUNNING);
        progressTracker.endSubTask();
        assertThat(leafTask.status()).isEqualTo(Status.FINISHED);

        progressTracker.beginSubTask();
        assertThat(progressTracker.currentSubTask()).isEqualTo(iterativeTask);
        assertThat(iterativeTask.status()).isEqualTo(Status.RUNNING);
        progressTracker.endSubTask();
        assertThat(iterativeTask.status()).isEqualTo(Status.FINISHED);

        assertThat(progressTracker.currentSubTask()).isEqualTo(rootTask);

        progressTracker.endSubTask();
        assertThat(rootTask.status()).isEqualTo(Status.FINISHED);
    }

    @Test
    void shouldNotThrowIfEndMoreTasksThanStarted() {
        var task = Tasks.leaf("leaf", new Concurrency(1));
        var log = new GdsTestLog();

        var progressTracker = TaskProgressTracker.create(
            new LoggerForProgressTrackingAdapter(log),
            task,
            new Concurrency(1),
            new JobId(),
            PlainSimpleRequestCorrelationId.create(),
            EmptyTaskRegistryFactory.INSTANCE
        );
        progressTracker.beginSubTask();
        progressTracker.endSubTask();
        assertThatNoException()
            .as("When `THROW_WHEN_USING_PROGRESS_TRACKER_WITHOUT_TASKS` is disabled (default state) we should not throw an exception.")
            .isThrownBy(progressTracker::endSubTask);

            Assertions
                .assertThat(log.getMessages(WARN))
                .extracting(removingThreadId())
                .containsExactly("leaf :: Tried to log progress, but there are no running tasks being tracked");
    }

    @Test
    void shouldUpdateProgress() {
        var task = Tasks.leaf("leaf", new Concurrency(1));
        var progressTracker = progressTracker(task);
        progressTracker.beginSubTask();
        progressTracker.onProgress(42);
        assertThat(task.getProgress().progress()).isEqualTo(42);
    }

    @Test
    void shouldCancelSubTasksOnDynamicIterative() {
        var task = Tasks.iterativeDynamic("iterative", new Concurrency(1), () -> List.of(Tasks.leaf("leaf", new Concurrency(1))), 2);
        var progressTracker = progressTracker(task);
        progressTracker.beginSubTask();
        assertThat(progressTracker.currentSubTask()).isEqualTo(task);

        // visit first iteration leaf
        progressTracker.beginSubTask();
        progressTracker.endSubTask();

        assertThat(task.subTasks()).extracting(Task::status).contains(Status.FINISHED);
        assertThat(task.subTasks()).extracting(Task::status).contains(Status.PENDING);

        // end task without visiting second iteration leaf
        progressTracker.endSubTask();
        assertThat(task.subTasks()).extracting(Task::status).contains(Status.FINISHED);
        assertThat(task.subTasks()).extracting(Task::status).contains(Status.CANCELED);
    }

    @Test
    void shouldLog100WhenTaskFinishedEarly() {
        try (var ignored = RenamesCurrentThread.renameThread("test")) {
            var task = Tasks.leaf("leaf", new Concurrency(1), 4);
            var log = new GdsTestLog();
            var progressTracker = TaskProgressTracker.create(
                new LoggerForProgressTrackingAdapter(log),
                task,
                new Concurrency(1),
                new JobId(),
                new RequestCorrelationIdForTesting("our request correlation id"),
                EmptyTaskRegistryFactory.INSTANCE
            );
            progressTracker.beginSubTask();
            progressTracker.onProgress();

            assertThat(log.getMessages(TestLog.INFO)).contains(
                "[our request correlation id] [test] leaf :: Start",
                "[our request correlation id] [test] leaf 25%"
            );

            progressTracker.endSubTask();

            assertThat(log.getMessages(TestLog.INFO)).contains(
                "[our request correlation id] [test] leaf 100%",
                "[our request correlation id] [test] leaf :: Finished"
            );
        }
    }

    @Test
    void shouldLog100OnlyOnLeafTasks() {
        try (var ignored = RenamesCurrentThread.renameThread("test")) {
            var task = Tasks.task("root", new Concurrency(1), Tasks.leaf("leaf", new Concurrency(1), 4));
            var log = new GdsTestLog();
            var progressTracker = TaskProgressTracker.create(
                new LoggerForProgressTrackingAdapter(log),
                task,
                new Concurrency(1),
                new JobId(),
                new RequestCorrelationIdForTesting("what request correlation id?"),
                EmptyTaskRegistryFactory.INSTANCE
            );

            progressTracker.beginSubTask(/*root*/);
            progressTracker.beginSubTask(/*leaf*/);
            progressTracker.onProgress();
            progressTracker.endSubTask(/*leaf*/);
            progressTracker.endSubTask(/*root*/);

            assertThat(log.getMessages(TestLog.INFO)).contains(
                "[what request correlation id?] [test] root :: Start",
                "[what request correlation id?] [test] root :: leaf :: Start",
                "[what request correlation id?] [test] root :: leaf 25%",
                "[what request correlation id?] [test] root :: leaf 100%",
                "[what request correlation id?] [test] root :: leaf :: Finished",
                "[what request correlation id?] [test] root :: Finished"
            );
        }
    }

    @Test
    void shouldRegisterBaseTaskOnBaseTaskStart() {
        var task = Tasks.leaf("root", new Concurrency(1));

        var taskStore = new PerDatabaseTaskStore(Duration.ZERO);
        var taskRegistry = new TaskRegistry("", taskStore);

        var progressTracker = TaskProgressTracker.create(
            LoggerForProgressTracking.noOpLog(),
            task,
            new Concurrency(1),
            new JobId(),
            PlainSimpleRequestCorrelationId.create(),
            jobId -> taskRegistry
        );

        assertThat(taskStore.query("")).isEmpty();

        progressTracker.beginSubTask();

        assertThat(taskStore.query("").map(UserTask::task)).contains(task);
    }

    @Test
    void stepsShouldGiveProgress() {
        var leafTask = Tasks.leaf("leaf", new Concurrency(1), 100);
        var progressTracker = progressTracker(leafTask);

        progressTracker.beginSubTaskWithSteps(13);
        progressTracker.onProgress(3);
        progressTracker.onSteps(1);
        double expectedDoubleProgressFromFirstStep = 100.0 / 13.0;
        long progressAfterFirstStep = leafTask.getProgress().progress();
        assertThat(progressAfterFirstStep).isEqualTo((long) expectedDoubleProgressFromFirstStep + 3);

        progressTracker.onProgress();
        progressTracker.onSteps(4);
        assertThat(leafTask.getProgress().progress()).isEqualTo(3 + 1 + (long) (100.0 * 5.0 / 13));
    }

    private TaskProgressTracker progressTracker(Task task, Log log) {
        return TaskProgressTracker.create(
            new LoggerForProgressTrackingAdapter(log),
            task,
            new Concurrency(1),
            new JobId(),
            PlainSimpleRequestCorrelationId.create(),
            EmptyTaskRegistryFactory.INSTANCE
        );
    }

    private TaskProgressTracker progressTracker(Task task) {
        return progressTracker(task, new GdsTestLog());
    }
}
