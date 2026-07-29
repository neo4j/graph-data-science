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
package org.neo4j.gds.progress.registration;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.progress.tasks.Status;
import org.neo4j.gds.progress.tasks.Task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskContainerTest {
    @Test
    void shouldAddTask() {
        var taskContainer = new TaskContainer();

        var task = mock(Task.class);
        taskContainer.add(task);

        assertThat(taskContainer.getAll()).containsExactly(task);
    }

    @Test
    void shouldAddSeveralTasks() {
        var taskContainer = new TaskContainer();

        var task1 = mock(Task.class);
        var task2 = mock(Task.class);
        var task3 = mock(Task.class);
        taskContainer.add(task1);
        taskContainer.add(task2);
        taskContainer.add(task3);

        assertThat(taskContainer.getAll()).containsExactly(task1, task2, task3);
    }

    @Test
    void shouldRemoveTasks() {
        var taskContainer = new TaskContainer();

        taskContainer.add(mock(Task.class));
        taskContainer.add(mock(Task.class));

        taskContainer.removeAll();

        assertThat(taskContainer.getAll()).isEmpty();
    }

    @Test
    void shouldMarkTasksAsCompleted() {
        var taskContainer = new TaskContainer();

        var task1 = mock(Task.class);
        var task2 = mock(Task.class);
        var task3 = mock(Task.class);
        var task4 = mock(Task.class);
        var task5 = mock(Task.class);
        taskContainer.add(task1);
        taskContainer.add(task2);
        taskContainer.add(task3);
        taskContainer.add(task4);
        taskContainer.add(task5);

        when(task1.status()).thenReturn(Status.CANCELED);
        when(task2.status()).thenReturn(Status.FAILED);
        when(task3.status()).thenReturn(Status.FINISHED);
        when(task4.status()).thenReturn(Status.PENDING);
        when(task5.status()).thenReturn(Status.RUNNING);
        var tasks = taskContainer.markAllCompleted();

        assertThat(tasks).containsExactly(task1, task2, task3, task4, task5);

        verify(task4).cancel();
        verify(task5).finish();
    }
}
