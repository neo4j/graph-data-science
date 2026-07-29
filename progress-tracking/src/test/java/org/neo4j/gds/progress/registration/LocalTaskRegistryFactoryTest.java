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
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.progress.tasks.LeafTask;
import org.neo4j.gds.progress.tasks.Tasks;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LocalTaskRegistryFactoryTest {
    @Test
    void shouldPutAndMarkCompletedDistinctTasks() {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);
        var taskRegistryFactory = new LocalTaskRegistryFactory(Log.noOpLog(), taskStore, User.DEFAULT);

        var task1 = Tasks.leaf("root1", new Concurrency(1));
        var taskRegistry1 = taskRegistryFactory.newInstance(new JobId());
        taskRegistry1.registerTask(task1);

        assertThat(taskStore.query(User.DEFAULT)).size().isEqualTo(1);

        var task2 = Tasks.leaf("root2", new Concurrency(1));
        var taskRegistry2 = taskRegistryFactory.newInstance(new JobId());
        taskRegistry2.registerTask(task2);

        assertThat(taskStore.query(User.DEFAULT)).size().isEqualTo(2);

        taskRegistry1.markCompleted();

        assertThat(taskStore.queryRunning().map(StoredTask::task)).contains(task2).doesNotContain(task1);
    }

    @Test
    void shouldThrowOnDuplicateJobId() {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);
        var taskRegistryFactory = new LocalTaskRegistryFactory(Log.noOpLog(), taskStore, User.DEFAULT);

        var jobId = new JobId();
        var task1 = Tasks.leaf("root1", new Concurrency(1));
        var taskRegistry1 = taskRegistryFactory.newInstance(jobId);
        taskRegistry1.registerTask(task1);

        assertThrows(IllegalArgumentException.class, () -> taskRegistryFactory.newInstance(jobId));
    }

    @Test
    void shouldAllowReplacingCompletedTasks() {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);
        var taskRegistryFactory = new LocalTaskRegistryFactory(Log.noOpLog(), taskStore, User.DEFAULT);

        var jobId = new JobId();
        var task1 = Tasks.leaf("root1", new Concurrency(1));
        var taskRegistry1 = taskRegistryFactory.newInstance(jobId);
        taskRegistry1.registerTask(task1);

        assertThrows(IllegalArgumentException.class, () -> taskRegistryFactory.newInstance(jobId));

        task1.start();
        assertThrows(IllegalArgumentException.class, () -> taskRegistryFactory.newInstance(jobId));

        task1.finish();

        var registry2 = taskRegistryFactory.newInstance(jobId);
        LeafTask task2 = Tasks.leaf("root2", new Concurrency(1));
        registry2.registerTask(task2);

        task2.start();
        task2.fail();

        var registry3 = taskRegistryFactory.newInstance(jobId);
        LeafTask task3 = Tasks.leaf("root3", new Concurrency(1));
        registry3.registerTask(task2);

        task3.start();
        task3.cancel();

        assertDoesNotThrow(() -> taskRegistryFactory.newInstance(jobId));
    }

    @Test
    void shouldAttachWhenJobExists() {
        var taskStore = mock(TaskStore.class);
        var taskRegistryFactory = new LocalTaskRegistryFactory(Log.noOpLog(), taskStore, User.DEFAULT);

        var jobId = new JobId();
        when(taskStore.lookup(User.DEFAULT, jobId)).thenReturn(Set.of(mock(StoredTask.class)));
        var taskRegistry = taskRegistryFactory.attach(jobId);

        assertNotNull(taskRegistry);
    }

    @Test
    void shouldNeverEverFailToAttachNoMatterWhatBecauseUsersWorkIsMoreImportantThanProgressTrackingNitPickings() {
        var log = mock(Log.class);
        var taskStore = mock(TaskStore.class);
        var taskRegistryFactory = new LocalTaskRegistryFactory(log, taskStore, User.DEFAULT);

        var jobId = new JobId("my expired job");
        when(taskStore.lookup(User.DEFAULT, jobId)).thenReturn(Collections.emptySet());
        var taskRegistry = taskRegistryFactory.attach(jobId);

        assertNotNull(taskRegistry);

        verify(log).warn("cannot attach to job 'my expired job'");
        verify(log).warn("falling back to overriding job 'my expired job'");
    }
}
