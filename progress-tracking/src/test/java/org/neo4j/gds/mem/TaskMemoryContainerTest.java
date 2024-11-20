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
package org.neo4j.gds.mem;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.UserTask;

import static org.assertj.core.api.Assertions.assertThat;

class TaskMemoryContainerTest {


    @Test
    void shouldReserve(){
        TaskMemoryContainer taskMemoryContainer=new TaskMemoryContainer();
        taskMemoryContainer.reserve("alice", "foo" , new JobId("JobId"), 10);
        taskMemoryContainer.reserve("alice", "foo2" , new JobId("JobId2"), 20);
        assertThat(taskMemoryContainer.taskReservedMemory()).isEqualTo(30L);

    }

    @Test
    void shouldRemove(){
        TaskMemoryContainer taskMemoryContainer=new TaskMemoryContainer();
        JobId jobId = new JobId("JobId");
        taskMemoryContainer.reserve("alice", "foo" , jobId, 10);
        taskMemoryContainer.reserve("alice", "foo2" , new JobId("JobId2"), 20);
        assertThat(taskMemoryContainer.taskReservedMemory()).isEqualTo(30L);
        taskMemoryContainer.removeTask(new UserTask("alice",jobId,null));
        assertThat(taskMemoryContainer.taskReservedMemory()).isEqualTo(20L);

    }

    @Test
    void shouldListForUser(){
        TaskMemoryContainer taskMemoryContainer=new TaskMemoryContainer();
        taskMemoryContainer.reserve("alice", "foo" , new JobId("JobId1"), 10);
        taskMemoryContainer.reserve("alice", "foo2" , new JobId("JobId2"), 15);
        taskMemoryContainer.reserve("bob", "foo3" , new JobId("JobId3"), 30);

        var aliceList = taskMemoryContainer.listTasks("alice").toList();
        assertThat(aliceList).hasSize(2);
        assertThat(aliceList.stream().map(UserEntityMemory::entity).toList()).containsExactlyInAnyOrder("foo","foo2");
        assertThat(aliceList.stream().map(UserEntityMemory::memoryInBytes).toList()).containsExactlyInAnyOrder(10L,15L);

    }

    @Test
    void shouldListAll(){
        TaskMemoryContainer taskMemoryContainer=new TaskMemoryContainer();
        taskMemoryContainer.reserve("alice", "foo" , new JobId("JobId1"), 10);
        taskMemoryContainer.reserve("alice", "foo2" , new JobId("JobId2"), 15);
        taskMemoryContainer.reserve("bob", "foo3" , new JobId("JobId3"), 20);
        var taskList =taskMemoryContainer.listTasks().toList();
        assertThat(taskList).hasSize(3);
        assertThat(taskList.stream().map(UserEntityMemory::entity).toList()).containsExactlyInAnyOrder("foo","foo2","foo3");
        assertThat(taskList.stream().map(UserEntityMemory::memoryInBytes).toList()).containsExactlyInAnyOrder(10L,15L,20L);

    }
}
