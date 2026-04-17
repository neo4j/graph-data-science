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
package org.neo4j.gds.memory.tracking;

import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreAddedEvent;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.utils.progress.UserTask;
import org.neo4j.gds.logging.Log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MemoryTrackerTest {
    @Test
    void shouldNotAllowNegativeInitialValue() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> MemoryTracker.create(Log.noOpLog(), -42))
            .withMessageContaining("Negative values are not allowed.");
    }

    @Test
    void shouldHaveAvailableMemorySameAsInitialBeforeAnyTracking() {
        var memoryTracker = new MemoryTracker(Log.noOpLog(), 19L);

        assertThat(memoryTracker.availableMemory())
            .isEqualTo(memoryTracker.availableMemory())
            .isEqualTo(19L);
    }

    @Test
    void shouldHaveAvailableMemoryWithoutTheTrackedMemory() {
        var memoryTracker = new MemoryTracker(Log.noOpLog(), 19L);

        memoryTracker.track("a", "b", new JobId("foo"), 9);
        memoryTracker.track("a", "b", new JobId("bar"), 3);

        assertThat(memoryTracker.availableMemory())
            .isEqualTo(memoryTracker.availableMemory())
            .isEqualTo(7L);
    }

    @Test
    void shouldListForUser(){
        var memoryTracker = new MemoryTracker(Log.noOpLog(), 19L);
        memoryTracker.track("alice","task1",new JobId("job1"), 9);
        memoryTracker.track("alice","task2",new JobId("job2"), 3);
        memoryTracker.track("bob","task3",new JobId("job3"), 5);
        memoryTracker.onGraphStoreAdded(new GraphStoreAddedEvent("alice","neo4j","graph1",11));
        var  aliceList = memoryTracker.listUser("alice").toList();
        assertThat(aliceList.stream().map(UserEntityMemory::name).toList()).containsExactlyInAnyOrder("task1","task2","graph1");
        assertThat(aliceList.stream().map(UserEntityMemory::entity).toList()).containsExactlyInAnyOrder("job1","job2","graph");

        assertThat(aliceList.stream().map(UserEntityMemory::memoryInBytes).toList()).containsExactlyInAnyOrder(9L,3L,11L);
    }

    @Test
    void shouldListForAll() {
        var memoryTracker = new MemoryTracker(Log.noOpLog(), 19L);
        memoryTracker.track("alice", "task1", new JobId("job1"), 9);
        memoryTracker.track("alice", "task2", new JobId("job2"), 3);
        memoryTracker.track("bob", "task3", new JobId("job3"), 5);
        memoryTracker.onGraphStoreAdded(new GraphStoreAddedEvent("alice", "neo4j", "graph1", 11));

        var list = memoryTracker.listAll().toList();
        assertThat(list.stream().map(UserEntityMemory::name).toList()).containsExactlyInAnyOrder("task1","task2","task3","graph1");
        assertThat(list.stream().map(UserEntityMemory::entity).toList()).containsExactlyInAnyOrder("job1","job2","job3","graph");

        assertThat(list.stream().map(UserEntityMemory::memoryInBytes).toList()).containsExactlyInAnyOrder(9L,3L,5L,11L);
    }

    @Test
    void shouldReturnMemoryForUser() {
        var memoryTracker = new MemoryTracker(Log.noOpLog(), 19L);
        memoryTracker.track("alice", "task1", new JobId("job1"), 9);
        memoryTracker.track("alice", "task2", new JobId("job2"), 3);
        memoryTracker.track("bob", "task3", new JobId("job3"), 5);
        memoryTracker.onGraphStoreAdded(new GraphStoreAddedEvent("alice", "neo4j", "graph1", 11));

        var aliceMemory = memoryTracker.memorySummary("alice");
        assertThat(aliceMemory.totalGraphsMemory()).isEqualTo(11L);
        assertThat(aliceMemory.totalTasksMemory()).isEqualTo(12L);

        var bobMemory = memoryTracker.memorySummary("bob");
        assertThat(bobMemory.totalGraphsMemory()).isEqualTo(0L);
        assertThat(bobMemory.totalTasksMemory()).isEqualTo(5L);

    }

    @Test
    void shouldReturnMemoryForAll() {
        var memoryTracker = new MemoryTracker(Log.noOpLog(), 19L);
        memoryTracker.track("alice", "task1", new JobId("job1"), 9);
        memoryTracker.track("alice", "task2", new JobId("job2"), 3);
        memoryTracker.track("bob", "task3", new JobId("job3"), 5);
        memoryTracker.onGraphStoreAdded(new GraphStoreAddedEvent("alice", "neo4j", "graph1", 11));

        var list = memoryTracker.memorySummary().toList();

        assertThat(list.stream()).map(UserMemorySummary::totalGraphsMemory).containsExactlyInAnyOrder(11L, 0L);
        assertThat(list.stream()).map(UserMemorySummary::totalTasksMemory).containsExactlyInAnyOrder(12L, 5L);

    }

    @Test
    void shouldFreeMemoryOnTaskCompleted() {
        var memoryTracker = new MemoryTracker(Log.noOpLog(), 19L);

        memoryTracker.track("a", "b", new JobId("foo"), 9);
        memoryTracker.track("a", "b", new JobId("bar"), 3);

        var userTaskMock = mock(UserTask.class, Answers.RETURNS_MOCKS);
        when(userTaskMock.jobId()).thenReturn(new JobId("foo"));
        when(userTaskMock.username()).thenReturn("a");

        memoryTracker.onTaskCompleted(userTaskMock);

        assertThat(memoryTracker.availableMemory())
            .isEqualTo(16L);
    }
}
