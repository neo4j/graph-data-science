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
package org.neo4j.graphalgo.core.utils.progress.v2.tasks;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskTest {

    @Test
    void startShouldSetStatusToRunning() {
        var task = Tasks.leaf("test");
        task.start();
        assertThat(task.status()).isEqualTo(Status.RUNNING);
    }

    @Test
    void startShouldOnlyTransitionFromOpen() {
        var task = Tasks.leaf("test");
        task.cancel();
        assertThatThrownBy(task::start)
            .hasMessageContaining("Task `test` with state CANCELED cannot be started");
    }

    @Test
    void finishShouldSetStatusToFinished() {
        var task = Tasks.leaf("test");
        task.start();
        task.finish();
        assertThat(task.status()).isEqualTo(Status.FINISHED);
    }

    @Test
    void finishShouldOnlyTransitionFromRunning() {
        var task = Tasks.leaf("test");
        assertThatThrownBy(task::finish)
            .hasMessageContaining("Task `test` with state PENDING cannot be finished");
    }

    @Test
    void cancelShouldSetStatusToFinished() {
        var task = Tasks.leaf("test");
        task.start();
        task.cancel();
        assertThat(task.status()).isEqualTo(Status.CANCELED);
    }

    @Test
    void cancelShouldNotBeCallableFromFinished() {
        var task = Tasks.leaf("test");
        task.start();
        task.finish();
        assertThatThrownBy(task::cancel)
            .hasMessageContaining("Task `test` with state FINISHED cannot be canceled");
    }

    @Test
    void nextSubtaskShouldReturnNextPendingTask() {
        var a = Tasks.leaf("A");
        var b = Tasks.leaf("B");
        var c = Tasks.leaf("C");

        var root = Tasks.task(
            "Root",
            a, b, c
        );

        assertThat(root.nextSubtask()).isEqualTo(a);

        a.start();
        a.finish();

        assertThat(root.nextSubtask()).isEqualTo(b);

        b.start();
        b.finish();

        assertThat(root.nextSubtask()).isEqualTo(c);
    }

    @Test
    void nextSubtaskShouldThrowIfPreviousTaskIsRunning() {
        var a = Tasks.leaf("A");
        var b = Tasks.leaf("B");
        var c = Tasks.leaf("C");

        var root = Tasks.task(
            "Root",
            a, b, c
        );

        assertThat(root.nextSubtask()).isEqualTo(a);

        a.start();

        assertThatThrownBy(root::nextSubtask).
            hasMessageContaining("some subtasks are still running");
    }

    @Test
    void nextSubtaskShouldThrowIfThereAreNoMoreOpenTasks() {
        var a = Tasks.leaf("A");

        var root = Tasks.task(
            "Root",
            a
        );

        assertThat(root.nextSubtask()).isEqualTo(a);

        a.start();
        a.finish();

        assertThatThrownBy(root::nextSubtask).
            hasMessageContaining("No more pending subtasks");
    }

    @Test
    void shouldGetCumulativeProgress() {
        var a = Tasks.leaf("A", 100);
        var b = Tasks.leaf("B", 100);

        var root = Tasks.task(
            "Root",
            a, b
        );

        assertThat(root.getProgress())
            .hasFieldOrPropertyWithValue("volume", 200L)
            .hasFieldOrPropertyWithValue("progress", 0L);

        a.logProgress(50);

        assertThat(root.getProgress())
            .hasFieldOrPropertyWithValue("volume", 200L)
            .hasFieldOrPropertyWithValue("progress", 50L);

        a.logProgress(50);
        a.logProgress(100);

        assertThat(root.getProgress())
            .hasFieldOrPropertyWithValue("volume", 200L)
            .hasFieldOrPropertyWithValue("progress", 200L);
    }

    @Test
    void shouldGetUnknownVolume() {
        var a = Tasks.leaf("A", 100);
        var b = Tasks.leaf("B");

        var root = Tasks.task(
            "Root",
            a, b
        );

        assertThat(root.getProgress())
            .hasFieldOrPropertyWithValue("volume", -1L)
            .hasFieldOrPropertyWithValue("progress", 0L);

        a.logProgress(50);

        assertThat(root.getProgress())
            .hasFieldOrPropertyWithValue("volume", -1L)
            .hasFieldOrPropertyWithValue("progress", 50L);
    }

    @Test
    void shouldSetVolumeLate() {
        var task = Tasks.task("root", Tasks.leaf("leaf"));
        assertThat(task.getProgress().volume()).isEqualTo(Task.UNKNOWN_VOLUME);
        task.nextSubtask().setVolume(100);
        assertThat(task.getProgress().volume()).isEqualTo(100);
    }
}
