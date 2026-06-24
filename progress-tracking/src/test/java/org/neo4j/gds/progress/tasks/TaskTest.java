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
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskTest {

    @Test
    void startShouldSetStatusToRunning() {
        var task = Tasks.leaf("test", new Concurrency(1));
        task.start();
        assertThat(task.status()).isEqualTo(Status.RUNNING);
    }

    @Test
    void startShouldOnlyTransitionFromOpen() {
        var task = Tasks.leaf("test", new Concurrency(1));
        task.cancel();
        assertThatThrownBy(task::start)
            .hasMessageContaining("Task `test` with state CANCELED cannot be started");
    }

    @Test
    void finishShouldSetStatusToFinished() {
        var task = Tasks.leaf("test", new Concurrency(1));
        task.start();
        task.finish();
        assertThat(task.status()).isEqualTo(Status.FINISHED);
    }

    @Test
    void finishShouldOnlyTransitionFromRunning() {
        var task = Tasks.leaf("test", new Concurrency(1));
        assertThatThrownBy(task::finish)
            .hasMessageContaining("Task `test` with state PENDING cannot be finished");
    }

    @Test
    void cancelShouldSetStatusToFinished() {
        var task = Tasks.leaf("test", new Concurrency(1));
        task.start();
        task.cancel();
        assertThat(task.status()).isEqualTo(Status.CANCELED);
    }

    @Test
    void cancelShouldNotBeCallableFromFinished() {
        var task = Tasks.leaf("test", new Concurrency(1));
        task.start();
        task.finish();
        assertThatThrownBy(task::cancel)
            .hasMessageContaining("Task `test` with state FINISHED cannot be canceled");
    }

    @Test
    void nextSubtaskShouldReturnNextPendingTask() {
        var a = Tasks.leaf("A", new Concurrency(1));
        var b = Tasks.leaf("B", new Concurrency(1));
        var c = Tasks.leaf("C", new Concurrency(1));

        var root = Tasks.task(
            "Root",
            new Concurrency(1), a, b, c
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
        var a = Tasks.leaf("A", new Concurrency(1));
        var b = Tasks.leaf("B", new Concurrency(1));
        var c = Tasks.leaf("C", new Concurrency(1));

        var root = Tasks.task(
            "Root",
            new Concurrency(1), a, b, c
        );
        root.start();

        assertThat(root.nextSubtask()).isEqualTo(a);

        a.start();

        assertThatThrownBy(root::nextSubtask).
            hasMessageContaining("some subtasks are still running");
    }

    @Test
    void nextSubtaskShouldThrowIfThereAreNoMoreOpenTasks() {
        var a = Tasks.leaf("A", new Concurrency(1));

        var root = Tasks.task(
            "Root",
            new Concurrency(1), a
        );
        root.start();

        assertThat(root.nextSubtask()).isEqualTo(a);

        a.start();
        a.finish();

        assertThatThrownBy(root::nextSubtask).
            hasMessageContaining("No more pending subtasks");
    }

    @Test
    void shouldGetCumulativeProgress() {
        var a = Tasks.leaf("A", new Concurrency(1), 100);
        var b = Tasks.leaf("B", new Concurrency(1), 100);

        var root = Tasks.task(
            "Root",
            new Concurrency(1), a, b
        );

        assertThat(root.getProgress()).isEqualTo(new Progress(0, 200));

        a.logProgress(50);

        assertThat(root.getProgress()).isEqualTo(new Progress(50, 200));

        a.logProgress(50);
        a.logProgress(100);

        assertThat(root.getProgress()).isEqualTo(new Progress(200, 200));
    }

    @Test
    void shouldGetUnknownVolume() {
        var a = Tasks.leaf("A", new Concurrency(1), 100);
        var b = Tasks.leaf("B", new Concurrency(1));

        var root = Tasks.task(
            "Root",
            new Concurrency(1), a, b
        );

        assertThat(root.getProgress()).isEqualTo(new Progress(0, -1L));

        a.logProgress(50);

        assertThat(root.getProgress()).isEqualTo(new Progress(50, -1));
    }

    @Test
    void shouldSetVolumeLate() {
        var task = Tasks.task("root", new Concurrency(1), Tasks.leaf("leaf", new Concurrency(1)));
        assertThat(task.getProgress().volume()).isEqualTo(Task.UNKNOWN_VOLUME);
        task.start();
        task.nextSubtask().setVolume(100);
        assertThat(task.getProgress().volume()).isEqualTo(100);
    }

    @Test
    void shouldSetProgressWhenFinishingTask() {
        var task = Tasks.iterativeOpen("root", new Concurrency(1), () -> List.of(Tasks.leaf("leaf", new Concurrency(1))));
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
        var task = Tasks.task("root", new Concurrency(1), Tasks.leaf("leaf", new Concurrency(1)));
        assertThatThrownBy(task::nextSubtask)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("`root` is not running");
    }

    @Test
    void shouldVisitTasks() {
        Task leafTask = Tasks.leaf("leaf", new Concurrency(1));
        Task intermediateTask = Tasks.task("root", new Concurrency(1), leafTask);
        Task iterativeTask = Tasks.iterativeFixed("iterative",
            new Concurrency(1),
            () -> List.of(Tasks.leaf("iterationLeaf", new Concurrency(1))), 2);

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
