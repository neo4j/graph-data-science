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
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IterativeTaskTest {

    @Test
    void shouldCreateSubtasks() {
        Supplier<List<Task>> taskSupplier = () -> List.of(
            Tasks.leaf("leaf1", new Concurrency(1)), Tasks.leaf(
                "leaf2",
                new Concurrency(1)
            )
        );
        var iterativeFixedTask = Tasks.iterativeFixed(
            "root",
            new Concurrency(1), taskSupplier,
            3
        );

        assertThat(iterativeFixedTask.subTasks()).hasSize(6);

        var iterativeDynamicTask = Tasks.iterativeDynamic(
            "root",
            new Concurrency(1), taskSupplier,
            3
        );

        assertThat(iterativeDynamicTask.subTasks()).hasSize(6);

        var iterativeOpenTask = Tasks.iterativeOpen(
            "root",
            new Concurrency(1), taskSupplier
        );

        assertThat(iterativeOpenTask.subTasks()).hasSize(0);
    }

    @Test
    void shouldCreateSubTasksOnDemandForOpen() {
        var leafTask = Tasks.leaf("leaf", new Concurrency(1));
        var openTask = Tasks.iterativeOpen("root", new Concurrency(1), () -> List.of(leafTask));
        openTask.start();
        assertThat(openTask.nextSubtask()).isEqualTo(leafTask);
    }

    @Test
    void nextSubtaskShouldThrowIfPreviousTaskIsRunning() {
        Supplier<List<Task>> taskSupplier = () -> List.of(Tasks.leaf("A", new Concurrency(1)));

        var root = Tasks.iterativeFixed("root", new Concurrency(1), taskSupplier, 3);

        root.start();
        root.nextSubtask().start();

        assertThatThrownBy(root::nextSubtask).hasMessageContaining(
            "Cannot move to next subtask, because subtask `A` is still running");
    }

    @Test
    void shouldReturnTheCorrectIteration() {
        Supplier<List<Task>> taskSupplier = () -> List.of(
            Tasks.leaf("A", new Concurrency(1)),
            Tasks.leaf("B", new Concurrency(1))
        );

        var root = Tasks.iterativeFixed("root", new Concurrency(1), taskSupplier, 3);
        root.start();

        assertThat(root.currentIteration()).isEqualTo(0);
        var subTask1 = root.nextSubtask();
        subTask1.start();
        subTask1.finish();

        assertThat(root.currentIteration()).isEqualTo(0);
        var subTask2 = root.nextSubtask();
        subTask2.start();
        subTask2.finish();

        root.nextSubtask();
        assertThat(root.currentIteration()).isEqualTo(1);
    }

    @Test
    void shouldNotStartSubtasksImplicitly() {
        Supplier<List<Task>> taskSupplier = () -> List.of(
            Tasks.iterativeFixed(
                "A",
                new Concurrency(1),
                () -> List.of(Tasks.leaf("leaf1", new Concurrency(1)), Tasks.leaf("leaf2", new Concurrency(1))), 1
            )
        );

        var root = Tasks.iterativeFixed("root", new Concurrency(1), taskSupplier, 3);
        root.start();

        assertThat(root.currentIteration()).isEqualTo(0);
        var subTask = root.nextSubtask();
        subTask.start();

        var leaf1 = subTask.nextSubtask();
        assertThat(leaf1.status()).isEqualTo(Status.PENDING);

        subTask.finish();
    }

}
