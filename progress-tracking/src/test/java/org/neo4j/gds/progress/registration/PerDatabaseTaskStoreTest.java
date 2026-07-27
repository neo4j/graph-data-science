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
import org.neo4j.gds.progress.tasks.LeafTask;
import org.neo4j.gds.progress.tasks.Tasks;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class PerDatabaseTaskStoreTest {

    @Test
    void shouldBeIdempotentOnRemove() {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);
        var jobId = new JobId();
        taskStore.store(User.DEFAULT, jobId, Tasks.leaf("leaf", new Concurrency(1)));
        taskStore.remove(User.DEFAULT, jobId);
        assertDoesNotThrow(() -> taskStore.remove(User.DEFAULT, jobId));
    }

    @Test
    void shouldReturnEmptyResultWhenStoreIsEmpty() {
        assertThat(PerDatabaseTaskStore.create(Duration.ZERO).query(User.DEFAULT))
            .isNotNull()
            .isEmpty();
    }

    @Test
    void shouldCountOngoingAcrossUsers() {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);

        var a = new User("a", false);
        taskStore.store(a, new JobId(), Tasks.leaf("v", new Concurrency(1)));
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(1);

        var b = new User("b", false);
        taskStore.store(b, new JobId(), Tasks.leaf("x", new Concurrency(1)));
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(2);

        taskStore.store(b, new JobId(), Tasks.leaf("y", new Concurrency(1)));
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(3);

        LeafTask failedTask = Tasks.leaf("y", new Concurrency(1));
        failedTask.fail();
        taskStore.store(b, new JobId(), failedTask);
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(3);

        LeafTask completedTask = Tasks.leaf("z", new Concurrency(1));
        completedTask.start();
        completedTask.finish();
        taskStore.store(b, new JobId(), completedTask);
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(3);

        LeafTask cancelledTask = Tasks.leaf("alpha", new Concurrency(1));
        cancelledTask.cancel();
        taskStore.store(b, new JobId(), cancelledTask);
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(3);
    }

    @Test
    void shouldCountAcrossUsers() {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);

        var a = new User("a", false);
        taskStore.store(a, new JobId(), Tasks.leaf("v", new Concurrency(1)));
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(1);

        var b = new User("b", false);
        taskStore.store(b, new JobId(), Tasks.leaf("x", new Concurrency(1)));
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(2);

        taskStore.store(b, new JobId(), Tasks.leaf("y", new Concurrency(1)));
        assertThat(taskStore.ongoingTaskCount()).isEqualTo(3);
    }

    @Test
    void shouldLookupByUser() {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);

        var alice = new User("alice", false);
        taskStore.store(alice, new JobId("42"), Tasks.leaf("leaf", new Concurrency(1)));
        taskStore.store(alice, new JobId("666"), Tasks.leaf("leaf", new Concurrency(1)));
        taskStore.store(new User("bob", false), new JobId("1337"), Tasks.leaf("other", new Concurrency(1)));

        assertThat(taskStore.query(alice)).hasSize(2)
            .allMatch(task -> task.user().equals(alice));

        assertThat(taskStore.lookup(alice, new JobId("42"))).isPresent()
            .get()
            .matches(task -> task.jobId().asString().equals("42"))
            .matches(task -> task.user().equals(alice));
    }

    @Test
    void shouldQueryAcrossUsers() {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);

        taskStore.store(new User("alice", false), new JobId("42"), Tasks.leaf("leaf", new Concurrency(1)));
        taskStore.store(new User("bob", false), new JobId("42"), Tasks.leaf("other", new Concurrency(1)));
        taskStore.store(new User("bob", false), new JobId("1337"), Tasks.leaf("other", new Concurrency(1)));

        assertThat(taskStore.query()).hasSize(3);
        assertThat(taskStore.query(new JobId("42"))).hasSize(2);
        assertThat(taskStore.query(new JobId(""))).hasSize(0);
    }

    @Test
    void shouldReturnEmptyOptionalForNonExistingUser() {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);

        var bogus = taskStore.lookup(new User("bogus", false), null);

        assertThat(bogus).isEmpty();
    }

    @Test
    void shouldReturnNonEmptyOptionalForExistingUser() {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);
        var aliceLeafTask = Tasks.leaf("leaf", new Concurrency(1));
        var alice = new User("alice", false);
        taskStore.store(alice, new JobId("42"), aliceLeafTask);
        taskStore.store(alice, new JobId("43"), Tasks.leaf("leaf_2", new Concurrency(1)));
        taskStore.store(new User("bob", false), new JobId("1337"), Tasks.leaf("other", new Concurrency(1)));

        var optionalAlice = taskStore.lookup(alice, new JobId("42"));
        assertThat(optionalAlice)
            .isPresent()
            .hasValue(new StoredTask(alice, new JobId("42"), aliceLeafTask));
    }

    @Test
    void shouldCleanupOnReachingLimit() throws InterruptedException {
        var taskStore = PerDatabaseTaskStore.create(Duration.ofMillis(100));

        var aliceLeafTask = Tasks.leaf("leaf", new Concurrency(1));
        JobId jobId = new JobId("42");
        var alice = new User("alice", false);
        taskStore.store(alice, jobId, aliceLeafTask);
        taskStore.markCompleted(alice, jobId);

        assertThat(taskStore.query()).hasSize(1);

        // wait for the cleanup to run
        Thread.sleep(Duration.ofMillis(200).toMillis());
        assertThat(taskStore.query()).isEmpty();
    }
}
