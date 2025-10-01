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
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.RenamesCurrentThread;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.PerDatabaseTaskStore;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.UserTask;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.utils.GdsFeatureToggles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.WARN;

class TaskProgressTrackerTest {

    @Test
    void shouldStepThroughSubtasks() {
        var leafTask = Tasks.leaf("leaf1");
        var iterativeTask = Tasks.iterativeFixed("iterative", () -> List.of(Tasks.leaf("leaf2")), 2);
        var rootTask = Tasks.task(
            "root",
            leafTask,
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
    void shouldThrowIfEndMoreTasksThanStartedAndFeatureToggleIsEnabled() {
        GdsFeatureToggles.FAIL_ON_PROGRESS_TRACKER_ERRORS.enableAndRun(() -> {
            var task = Tasks.leaf("leaf");
            TaskProgressTracker progressTracker = progressTracker(task);
            progressTracker.beginSubTask();
            progressTracker.endSubTask();
            assertThatThrownBy(progressTracker::endSubTask)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Tried to log progress, but there are no running tasks being tracked");
        });
    }

    @Test
    void shouldNotThrowIfEndMoreTasksThanStarted() {
        GdsFeatureToggles.FAIL_ON_PROGRESS_TRACKER_ERRORS.disableAndRun(() -> {
            var task = Tasks.leaf("leaf");
            var log = new GdsTestLog();
            var progressTracker = new TaskProgressTracker(task, log, new Concurrency(1), EmptyTaskRegistryFactory.INSTANCE);
            progressTracker.beginSubTask();
            progressTracker.endSubTask();
            assertThatNoException()
                .as("When `THROW_WHEN_USING_PROGRESS_TRACKER_WITHOUT_TASKS` is disabled (default state) we should not throw an exception.")
                .isThrownBy(progressTracker::endSubTask);

            Assertions
                .assertThat(log.getMessages(WARN))
                .extracting(removingThreadId())
                .containsExactly("leaf :: Tried to log progress, but there are no running tasks being tracked");
        });
    }

    @Test
    void shouldLogProgress() {
        var task = Tasks.leaf("leaf");
        var progressTracker = progressTracker(task);
        progressTracker.beginSubTask();
        progressTracker.logProgress(42);
        assertThat(task.getProgress().progress()).isEqualTo(42);
    }

    @Test
    void shouldCancelSubTasksOnDynamicIterative() {
        var task = Tasks.iterativeDynamic("iterative", () -> List.of(Tasks.leaf("leaf")), 2);
        var progressTracker = progressTracker(task);
        progressTracker.beginSubTask();
        assertThat(progressTracker.currentSubTask()).isEqualTo(task);

        var iterativeSubTasks = task.subTasks();

        // visit first iteration leaf
        progressTracker.beginSubTask();
        progressTracker.endSubTask();

        assertThat(iterativeSubTasks).extracting(Task::status).contains(Status.FINISHED);
        assertThat(iterativeSubTasks).extracting(Task::status).contains(Status.PENDING);

        // end task without visiting second iteration leaf
        progressTracker.endSubTask();
        assertThat(iterativeSubTasks).extracting(Task::status).contains(Status.FINISHED);
        assertThat(iterativeSubTasks).extracting(Task::status).contains(Status.CANCELED);
    }

    @Test
    void shouldAssertFailureOnExpectedSubTaskSubString() {
        var task = Tasks.task("Foo", Tasks.leaf("Leaf1"));
        var progressTracker = progressTracker(task);

        assertThatThrownBy(() -> progressTracker.beginSubTask("Bar"))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("Expected task name to contain `Bar`, but was `Foo`");

        assertThatThrownBy(() -> progressTracker.beginSubTask("Leaf2"))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("Expected task name to contain `Leaf2`, but was `Leaf1`");

        assertThatThrownBy(() -> progressTracker.endSubTask("Leaf2"))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("Expected task name to contain `Leaf2`, but was `Leaf1`");

        progressTracker.endSubTask(); // call once manually as the call before didn't execute due to the assertion error
        assertThatThrownBy(() -> progressTracker.endSubTask("Bar"))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("Expected task name to contain `Bar`, but was `Foo`");
    }

    @Test
    void shouldAssertSuccessOnExpectedSuBTaskSubString() {
        var task = Tasks.task("Foo", Tasks.leaf("Leaf"));
        var progressTracker = progressTracker(task);

        assertDoesNotThrow(() -> progressTracker.beginSubTask("Foo"));
        assertDoesNotThrow(() -> progressTracker.beginSubTask("Leaf"));
        assertDoesNotThrow(() -> progressTracker.endSubTask("Leaf"));
        assertDoesNotThrow(() -> progressTracker.endSubTask("Foo"));
    }

    @Test
    void shouldLog100WhenTaskFinishedEarly() {
        try (var ignored = RenamesCurrentThread.renameThread("test")) {
            var task = Tasks.leaf("leaf", 4);
            var log = new GdsTestLog();
            var progressTracker = new TaskProgressTracker(task, log, new Concurrency(1), new JobId("our job id"), EmptyTaskRegistryFactory.INSTANCE, EmptyUserLogRegistryFactory.INSTANCE);
            progressTracker.beginSubTask();
            progressTracker.logProgress(1);

            assertThat(log.getMessages(TestLog.INFO)).contains(
                "[our job id] [test] leaf :: Start",
                "[our job id] [test] leaf 25%"
            );

            progressTracker.endSubTask();

            assertThat(log.getMessages(TestLog.INFO)).contains(
                "[our job id] [test] leaf 100%",
                "[our job id] [test] leaf :: Finished"
            );
        }
    }

    @Test
    void shouldLog100OnlyOnLeafTasks() {
        try (var ignored = RenamesCurrentThread.renameThread("test")) {
            var task = Tasks.task("root", Tasks.leaf("leaf", 4));
            var log = new GdsTestLog();
            var progressTracker = new TaskProgressTracker(task, log, new Concurrency(1), new JobId("what job id?"), EmptyTaskRegistryFactory.INSTANCE, EmptyUserLogRegistryFactory.INSTANCE);

            progressTracker.beginSubTask("root");
            progressTracker.beginSubTask("leaf");
            progressTracker.logProgress(1);
            progressTracker.endSubTask("leaf");
            progressTracker.endSubTask("root");

            assertThat(log.getMessages(TestLog.INFO)).contains(
                "[what job id?] [test] root :: Start",
                "[what job id?] [test] root :: leaf :: Start",
                "[what job id?] [test] root :: leaf 25%",
                "[what job id?] [test] root :: leaf 100%",
                "[what job id?] [test] root :: leaf :: Finished",
                "[what job id?] [test] root :: Finished"
            );
        }
    }

    @Test
    void shouldRegisterBaseTaskOnBaseTaskStart() {
        var task = Tasks.leaf("root");

        var taskStore = new PerDatabaseTaskStore();
        var taskRegistry = new TaskRegistry("", taskStore);
        var progressTracker = new TaskProgressTracker(task, org.neo4j.gds.logging.Log.noOpLog(), new Concurrency(1), jobId -> taskRegistry);

        assertThat(taskStore.query("")).isEmpty();

        progressTracker.beginSubTask();

        assertThat(taskStore.query("").map(UserTask::task)).contains(task);
    }

    @Test
    void stepsShouldGiveProgress() {
        var leafTask = Tasks.leaf("leaf", 100);
        var progressTracker = progressTracker(leafTask);

        progressTracker.beginSubTask();
        progressTracker.setSteps(13);
        progressTracker.logProgress(3);
        progressTracker.logSteps(1);
        double expectedDoubleProgressFromFirstStep = 100.0 / 13.0;
        long progressAfterFirstStep = leafTask.getProgress().progress();
        assertThat(progressAfterFirstStep).isEqualTo((long) expectedDoubleProgressFromFirstStep + 3);

        progressTracker.logProgress(1);
        progressTracker.logSteps(4);
        assertThat(leafTask.getProgress().progress()).isEqualTo(3 + 1 + (long) (100.0 * 5.0 / 13));
    }

    @Test
    void shouldLogRightLevel() {
        var log = new GdsTestLog();
        var leafTask = Tasks.leaf("leaf", 100);
        var progressTracker = progressTracker(leafTask, log);

        progressTracker.logMessage(LogLevel.WARNING, "WARNING MESSAGE 0");
        progressTracker.logWarning("WARNING MESSAGE 1");
        progressTracker.logMessage(LogLevel.INFO, "INFO MESSAGE 0");
        progressTracker.logInfo("INFO MESSAGE 1");
        progressTracker.logMessage(LogLevel.DEBUG, "DEBUG MESSAGE 0");
        progressTracker.logDebug("DEBUG MESSAGE 1");

        assertThat(log.getMessages(TestLog.WARN).size()).isEqualTo(2);
        assertThat(log.getMessages(TestLog.WARN).get(0)).contains("WARNING MESSAGE 0");
        assertThat(log.getMessages(TestLog.WARN).get(1)).contains("WARNING MESSAGE 1");
        assertThat(log.getMessages(TestLog.INFO).size()).isEqualTo(2);
        assertThat(log.getMessages(TestLog.INFO).get(0)).contains("INFO MESSAGE 0");
        assertThat(log.getMessages(TestLog.INFO).get(1)).contains("INFO MESSAGE 1");
        assertThat(log.getMessages(TestLog.DEBUG).size()).isEqualTo(2);
        assertThat(log.getMessages(TestLog.DEBUG).get(0)).contains("DEBUG MESSAGE 0");
        assertThat(log.getMessages(TestLog.DEBUG).get(1)).contains("DEBUG MESSAGE 1");
    }

    private TaskProgressTracker progressTracker(Task task, Log log) {
        return new TaskProgressTracker(task, log, new Concurrency(1), EmptyTaskRegistryFactory.INSTANCE);
    }

    private TaskProgressTracker progressTracker(Task task) {
        return progressTracker(task, new GdsTestLog());
    }
}
