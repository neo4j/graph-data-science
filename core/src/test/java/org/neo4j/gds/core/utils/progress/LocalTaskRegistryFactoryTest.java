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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocalTaskRegistryFactoryTest {

    TaskStore taskStore;
    TaskRegistryFactory taskRegistryFactory;

    @BeforeEach
    void setup() {
        this.taskStore = new GlobalTaskStore();
        this.taskRegistryFactory = new LocalTaskRegistryFactory("", taskStore);
    }

    @Test
    void shouldPutAndRemoveDistinctTasks() {
        var task1 = Tasks.leaf("root1");
        var taskRegistry1 = taskRegistryFactory.newInstance(new JobId());
        taskRegistry1.registerTask(task1);

        assertThat(taskStore.query("")).size().isEqualTo(1);

        var task2 = Tasks.leaf("root2");
        var taskRegistry2 = taskRegistryFactory.newInstance(new JobId());
        taskRegistry2.registerTask(task2);

        assertThat(taskStore.query("")).size().isEqualTo(2);

        taskRegistry1.unregisterTask();

        assertThat(taskStore.query("").map(TaskStore.UserTask::task)).contains(task2).doesNotContain(task1);
    }

    @Test
    void shouldThrowOnDuplicateJobId() {
        var jobId = new JobId();

        var task1 = Tasks.leaf("root1");
        var taskRegistry1 = taskRegistryFactory.newInstance(jobId);
        taskRegistry1.registerTask(task1);

        assertThrows(IllegalArgumentException.class, () -> taskRegistryFactory.newInstance(jobId));
    }
}
