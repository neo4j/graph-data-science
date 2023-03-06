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
package org.neo4j.gds.core.utils.progress;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.progress.TaskStore.UserTask;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import static org.assertj.core.api.Assertions.assertThat;

class TaskRegistryTest {

    @Test
    void shouldStoreIncomingTasks() {
        var globalTaskStore = new GlobalTaskStore();
        var taskRegistry1 = new TaskRegistry("", globalTaskStore);

        assertThat(globalTaskStore.isEmpty()).isTrue();

        var task1 = Tasks.leaf("task1");
        taskRegistry1.registerTask(task1);

        assertThat(globalTaskStore.query("").map(UserTask::task)).contains(task1);
        assertThat(globalTaskStore.isEmpty()).isFalse();

        var taskRegistry2 = new TaskRegistry("", globalTaskStore);
        var task2 = Tasks.leaf("task2");
        taskRegistry2.registerTask(task2);

        assertThat(globalTaskStore.query("").map(UserTask::task)).contains(task1, task2);
        assertThat(globalTaskStore.isEmpty()).isFalse();
    }

    @Test
    void shouldRemoveStoredTasks() {
        var globalTaskStore = new GlobalTaskStore();
        var taskRegistry = new TaskRegistry("", globalTaskStore);

        var task = Tasks.leaf("task");
        taskRegistry.registerTask(task);

        assertThat(globalTaskStore.isEmpty()).isFalse();

        var jobId = globalTaskStore.query("").map(UserTask::jobId).iterator().next();
        globalTaskStore.remove("", jobId);

        assertThat(globalTaskStore.isEmpty()).isTrue();
    }

    @Test
    void shouldDetectAlreadyRegisteredTasks() {
        var globalTaskStore = new GlobalTaskStore();
        var taskRegistry = new TaskRegistry("", globalTaskStore);

        var task = Tasks.leaf("task");

        assertThat(taskRegistry.containsTask(task)).isFalse();

        taskRegistry.registerTask(task);

        assertThat(taskRegistry.containsTask(task)).isTrue();
    }

}
