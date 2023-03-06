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

import org.junit.jupiter.api.Test;

import java.util.List;

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
        root.start();

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
        root.start();

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
        root.start();

        assertThat(root.nextSubtask()).isEqualTo(a);

        a.start();
        a.finish();

        assertThatThrownBy(root::nextSubtask).
            hasMessageContaining("No more pending subtasks");
    }

    @Test
    void shouldSetConcurrencyWhenApplicable() {
        int concurrency = 42;

        var a = Tasks.leaf("A");
        var b = Tasks.task("B", a);
        var c = Tasks.leaf("C");
        var d = Tasks.task("C", b, c);

        c.setMaxConcurrency(concurrency + 1);
        d.setMaxConcurrency(concurrency);

        assertThat(a.maxConcurrency()).isEqualTo(concurrency);
        assertThat(b.maxConcurrency()).isEqualTo(concurrency);
        assertThat(c.maxConcurrency()).isEqualTo(concurrency + 1);
        assertThat(d.maxConcurrency()).isEqualTo(concurrency);
    }

    @Test
    void shouldGetCumulativeProgress() {
        var a = Tasks.leaf("A", 100);
        var b = Tasks.leaf("B", 100);

        var root = Tasks.task(
            "Root",
            a, b
        );

        assertThat(root.getProgress()).isEqualTo(ImmutableProgress.of(0, 200));

        a.logProgress(50);

        assertThat(root.getProgress()).isEqualTo(ImmutableProgress.of(50, 200));

        a.logProgress(50);
        a.logProgress(100);

        assertThat(root.getProgress()).isEqualTo(ImmutableProgress.of(200, 200));
    }

    @Test
    void shouldGetUnknownVolume() {
        var a = Tasks.leaf("A", 100);
        var b = Tasks.leaf("B");

        var root = Tasks.task(
            "Root",
            a, b
        );

        assertThat(root.getProgress()).isEqualTo(ImmutableProgress.of(0, -1L));

        a.logProgress(50);

        assertThat(root.getProgress()).isEqualTo(ImmutableProgress.of(50, -1));
    }

    @Test
    void shouldSetVolumeLate() {
        var task = Tasks.task("root", Tasks.leaf("leaf"));
        assertThat(task.getProgress().volume()).isEqualTo(Task.UNKNOWN_VOLUME);
        task.start();
        task.nextSubtask().setVolume(100);
        assertThat(task.getProgress().volume()).isEqualTo(100);
    }

    @Test
    void shouldSetProgressWhenFinishingTask() {
        var task = Tasks.iterativeOpen("root", () -> List.of(Tasks.leaf("leaf")));
        task.start();
        var leaf1 = task.nextSubtask();
        leaf1.start();
        leaf1.logProgress(22L);
        leaf1.finish();

        assertThat(leaf1.getProgress().progress()).isEqualTo(22L);
        assertThat(leaf1.getProgress().volume()).isEqualTo(22L);
        assertThat(task.getProgress().volume()).isEqualTo(Task.UNKNOWN_VOLUME);

        var leaf2 = task.nextSubtask();
        leaf2.start();
        leaf2.setVolume(20L);
        leaf2.finish();

        assertThat(leaf2.getProgress().progress()).isEqualTo(20L);
        assertThat(leaf2.getProgress().volume()).isEqualTo(20L);
        assertThat(task.getProgress().volume()).isEqualTo(Task.UNKNOWN_VOLUME);

        task.finish();
        assertThat(task.getProgress().volume()).isEqualTo(42L);
    }


    @Test
    void shouldNotProgressWhenNotStarted() {
        var task = Tasks.task("root", Tasks.leaf("leaf"));
        assertThatThrownBy(task::nextSubtask)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("`root` is not running");
    }

    @Test
    void shouldVisitTasks() {
        Task leafTask = Tasks.leaf("leaf");
        Task intermediateTask = Tasks.task("root", leafTask);
        Task iterativeTask = Tasks.iterativeFixed("iterative", () -> List.of(Tasks.leaf("iterationLeaf")), 2);

        var taskVisitor = new CountingTaskVisitor();

        assertThat(taskVisitor.visitIntermediateTaskInvocations).isEqualTo(0);
        assertThat(taskVisitor.visitLeafTaskInvocations).isEqualTo(0);
        assertThat(taskVisitor.visitIterativeTaskInvocations).isEqualTo(0);

        leafTask.visit(taskVisitor);
        intermediateTask.visit(taskVisitor);
        iterativeTask.visit(taskVisitor);

        assertThat(taskVisitor.visitIntermediateTaskInvocations).isEqualTo(1);
        assertThat(taskVisitor.visitLeafTaskInvocations).isEqualTo(1);
        assertThat(taskVisitor.visitIterativeTaskInvocations).isEqualTo(1);
    }

    static class CountingTaskVisitor implements TaskVisitor {

        int visitLeafTaskInvocations = 0;
        int visitIntermediateTaskInvocations = 0;
        int visitIterativeTaskInvocations = 0;

        @Override
        public void visitLeafTask(LeafTask leafTask) {
            visitLeafTaskInvocations++;
        }

        @Override
        public void visitIntermediateTask(Task task) {
            visitIntermediateTaskInvocations++;
        }

        @Override
        public void visitIterativeTask(IterativeTask iterativeTask) {
            visitIterativeTaskInvocations++;
        }
    }
}
