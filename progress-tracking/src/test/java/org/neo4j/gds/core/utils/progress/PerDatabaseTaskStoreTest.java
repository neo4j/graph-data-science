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
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.utils.progress.tasks.LeafTask;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class PerDatabaseTaskStoreTest {

    @Test
    void shouldBeIdempotentOnRemove() {
        var taskStore = new PerDatabaseTaskStore(Duration.ZERO);
        var jobId = new JobId();
        taskStore.store("", jobId, Tasks.leaf("leaf"));
        taskStore.remove("", jobId);
        assertDoesNotThrow(() -> taskStore.remove("", jobId));
    }

    @Test
    void shouldReturnEmptyResultWhenStoreIsEmpty() {
        assertThat(new PerDatabaseTaskStore(Duration.ZERO).query(""))
            .isNotNull()
            .isEmpty();
    }

    @Test
    void shouldCountOngoingAcrossUsers() {
        var taskStore = new PerDatabaseTaskStore(Duration.ZERO);

        taskStore.store("a", new JobId(), Tasks.leaf("v"));
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(1);

        taskStore.store("b", new JobId(), Tasks.leaf("x"));
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(2);

        taskStore.store("b", new JobId(), Tasks.leaf("y"));
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(3);

        LeafTask failedTask = Tasks.leaf("y");
        failedTask.fail();
        taskStore.store("b", new JobId(), failedTask);
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(3);

        LeafTask completedTask = Tasks.leaf("z");
        completedTask.start();
        completedTask.finish();
        taskStore.store("b", new JobId(), completedTask);
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(3);

        LeafTask cancelledTask = Tasks.leaf("alpha");
        cancelledTask.cancel();
        taskStore.store("b", new JobId(), cancelledTask);
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(3);
    }

    @Test
    void shouldCountAcrossUsers() {
        var taskStore = new PerDatabaseTaskStore(Duration.ZERO);
        taskStore.store("a", new JobId(), Tasks.leaf("v"));
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(1);

        taskStore.store("b", new JobId(), Tasks.leaf("x"));
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(2);

        taskStore.store("b", new JobId(), Tasks.leaf("y"));
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(3);
    }

    @Test
    void shouldQueryByUser() {
        var taskStore = new PerDatabaseTaskStore(Duration.ZERO);
        taskStore.store("alice", new JobId("42"), Tasks.leaf("leaf"));
        taskStore.store("alice", new JobId("666"), Tasks.leaf("leaf"));
        taskStore.store("bob", new JobId("1337"), Tasks.leaf("other"));

        assertThat(taskStore.query("alice")).hasSize(2)
            .allMatch(task -> task.username().equals("alice"));

        assertThat(taskStore.query("alice", new JobId("42"))).isPresent()
            .get()
            .matches(task -> task.jobId().asString().equals("42"))
            .matches(task -> task.username().equals("alice"));
    }

    @Test
    void shouldQueryMultipleUsers() {
        var taskStore = new PerDatabaseTaskStore(Duration.ZERO);
        taskStore.store("alice", new JobId("42"), Tasks.leaf("leaf"));
        taskStore.store("bob", new JobId("1337"), Tasks.leaf("other"));

        assertThat(taskStore.query()).hasSize(2);
        assertThat(taskStore.query(new JobId("42"))).hasSize(1);
        assertThat(taskStore.query(new JobId(""))).hasSize(0);
    }

    @Test
    void shouldReturnEmptyOptionalForNonExistingUser() {
        var taskStore = new PerDatabaseTaskStore(Duration.ZERO);

        var bogus = taskStore.query("bogus", null);

        assertThat(bogus).isEmpty();
    }

    @Test
    void shouldReturnNonEmptyOptionalForExistingUser() {
        var taskStore = new PerDatabaseTaskStore(Duration.ZERO);
        var aliceLeafTask = Tasks.leaf("leaf");
        taskStore.store("alice", new JobId("42"), aliceLeafTask);
        taskStore.store("alice", new JobId("43"), Tasks.leaf("leaf_2"));
        taskStore.store("bob", new JobId("1337"), Tasks.leaf("other"));

        var alice = taskStore.query("alice", new JobId("42"));
        assertThat(alice)
            .isPresent()
            .hasValue(new UserTask("alice", new JobId("42"), aliceLeafTask));
    }

    @Test
    void shouldCleanupOnReachingLimit() throws InterruptedException {
        var taskStore = new PerDatabaseTaskStore(Duration.ofMillis(100));

        var aliceLeafTask = Tasks.leaf("leaf");
        JobId jobId = new JobId("42");
        taskStore.store("alice", jobId, aliceLeafTask);
        taskStore.markCompleted("alice", jobId);

        assertThat(taskStore.query()).hasSize(1);

        // wait for the cleanup to run
        Thread.sleep(Duration.ofMillis(200).toMillis());
        assertThat(taskStore.query()).isEmpty();
    }
}
