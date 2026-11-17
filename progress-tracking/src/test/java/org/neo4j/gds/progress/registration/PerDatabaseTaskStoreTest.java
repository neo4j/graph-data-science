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
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class PerDatabaseTaskStoreTest {

    @Test
    void  markCompleteShouldNotifyManyListeners(){
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);
        var listener = new TaskStoreListener(){
            public int val = 0;
            @Override
            public void onTaskAdded(StoredTask storedTask) {

            }

            @Override
            public void  onTaskCompleted(StoredTask storedTask) {
                val++;
            }
        };
        taskStore.addListener(listener);

        JobId jobId = new JobId();
        taskStore.store(User.DEFAULT, jobId, Tasks.leaf("leaf", new Concurrency(1)));

        taskStore.markCompleted(User.DEFAULT, jobId);

        assertThat(listener.val).isEqualTo(1);

    }

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

        assertThat(taskStore.lookup(alice, new JobId("42"))).first()
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

        var tasks = taskStore.lookup(alice, new JobId("42"));
        assertThat(tasks).containsExactly(new StoredTask(alice, new JobId("42"), aliceLeafTask));
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
        assertThat(taskStore.registeredJobCount()).isZero();
    }

    @Test
    void shouldDropEntryOnRemove() {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);
        var alice = new User("alice", false);
        var jobId = new JobId("42");
        taskStore.store(alice, jobId, Tasks.leaf("a", new Concurrency(1)));
        taskStore.store(alice, jobId, Tasks.leaf("b", new Concurrency(1)));
        taskStore.store(alice, new JobId("43"), Tasks.leaf("c", new Concurrency(1)));
        assertThat(taskStore.registeredJobCount()).isEqualTo(2);

        taskStore.remove(alice, jobId);

        assertThat(taskStore.registeredJobCount()).isEqualTo(1);
        assertThat(taskStore.lookup(alice, jobId)).isEmpty();
        assertThat(taskStore.query()).hasSize(1);
    }

    @Test
    void shouldDropEntryWhenCompletedTasksAreCleanedUp() {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);

        for (int i = 0; i < 100; i++) {
            var jobId = new JobId();
            taskStore.store(User.DEFAULT, jobId, Tasks.leaf("leaf", new Concurrency(1)));
            taskStore.markCompleted(User.DEFAULT, jobId);
        }

        assertThat(taskStore.registeredJobCount()).isZero();
    }

    @Test
    void shouldNotCreateEntriesForUnknownKeys() {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);
        var unknownUser = new User("unknown", false);
        var unknownJob = new JobId("unknown");

        assertThat(taskStore.lookup(unknownUser, unknownJob)).isEmpty();
        taskStore.remove(unknownUser, unknownJob);
        taskStore.markCompleted(unknownUser, unknownJob);

        assertThat(taskStore.registeredJobCount()).isZero();
        assertThat(taskStore.query()).isEmpty();
    }

    @Test
    void shouldStayConsistentUnderConcurrentStoreAndRemove() throws Exception {
        var taskStore = PerDatabaseTaskStore.create(Duration.ZERO);
        var keyCount = 8;
        var jobIds = IntStream.range(0, keyCount).mapToObj(i -> new JobId("job-" + i)).toList();
        var iterations = 2_000;
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(6);

        try {
            var futures = new ArrayList<Future<?>>();
            for (int t = 0; t < 3; t++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < iterations; i++) {
                        taskStore.store(User.DEFAULT, jobIds.get(i % keyCount), Tasks.leaf("leaf", new Concurrency(1)));
                    }
                    return null;
                }));
            }
            for (int t = 0; t < 2; t++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < iterations; i++) {
                        taskStore.remove(User.DEFAULT, jobIds.get(i % keyCount));
                    }
                    return null;
                }));
            }
            futures.add(executor.submit(() -> {
                start.await();
                for (int i = 0; i < iterations; i++) {
                    taskStore.lookup(User.DEFAULT, new JobId("unknown-" + i));
                    taskStore.remove(User.DEFAULT, new JobId("unknown-" + i));
                }
                return null;
            }));

            start.countDown();
            for (var future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        // only known keys may be present, and no entry may be an empty container
        var nonEmptyJobs = jobIds.stream().filter(jobId -> !taskStore.lookup(User.DEFAULT, jobId).isEmpty()).count();
        assertThat(taskStore.registeredJobCount()).isEqualTo(nonEmptyJobs);

        // a store after the removals have finished is always visible
        for (var jobId : jobIds) {
            var task = Tasks.leaf("final", new Concurrency(1));
            taskStore.store(User.DEFAULT, jobId, task);
            assertThat(taskStore.lookup(User.DEFAULT, jobId)).contains(new StoredTask(User.DEFAULT, jobId, task));
        }

        jobIds.forEach(jobId -> taskStore.remove(User.DEFAULT, jobId));
        assertThat(taskStore.registeredJobCount()).isZero();
        assertThat(taskStore.query()).isEmpty();
    }
}
