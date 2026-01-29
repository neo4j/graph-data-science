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
package org.neo4j.gds.procedures.operations;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.operations.OperationsApplications;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.UserTask;
import org.neo4j.gds.core.utils.progress.tasks.LeafTask;
import org.neo4j.gds.core.utils.warnings.UserLogEntry;
import org.neo4j.gds.core.utils.warnings.UserLogStore;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LocalOperationsProcedureFacadeTest {

    @Test
    void shouldQueryProgressOnlyOngoing() {
        var taskStore = mock(TaskStore.class);
        var operationsApplications = OperationsApplications.create(
            null,
            RequestScopedDependencies.builder()
                .taskStore(taskStore)
                .user(new User("alice", false))
                .build()
        );

        var applicationsFacade = mock(ApplicationsFacade.class);
        when(applicationsFacade.operations()).thenReturn(operationsApplications);

        var operationsProcedureFacade = new LocalOperationsProcedureFacade(applicationsFacade);

        var finished = new LeafTask("t1", 1);
        finished.start();
        finished.finish();

        var cancelled = new LeafTask("t2", 1);
        cancelled.cancel();

        var running = new LeafTask("t3", 1);
        running.start();

        var pending = new LeafTask("t4", 1);

        var failed = new LeafTask("t5", 1);
        failed.fail();


        var jobId = new JobId("a job id");
        var mockedTasks = Stream.of(
            new UserTask("alice", jobId, pending),
            new UserTask("alice", jobId, running),
            new UserTask("alice", jobId, failed),
            new UserTask("alice", jobId, cancelled),
            new UserTask("alice", jobId, finished)
        );
        when(taskStore.query("alice")).thenReturn(mockedTasks);

        var actualProgress = operationsProcedureFacade.listProgress("", false);
        assertThat(actualProgress).map(ProgressResult::taskName).containsExactlyInAnyOrder(
            pending.description(),
            running.description()
        );
    }

    @Test
    void shouldQueryProgress() {
        var taskStore = mock(TaskStore.class);
        var operationsApplications = OperationsApplications.create(
            null,
            RequestScopedDependencies.builder()
                .taskStore(taskStore)
                .user(new User("alice", false))
                .build()
        );

        var applicationsFacade = mock(ApplicationsFacade.class);
        when(applicationsFacade.operations()).thenReturn(operationsApplications);

        var operationsProcedureFacade = new LocalOperationsProcedureFacade(applicationsFacade);

        var finished = new LeafTask("t1", 1);
        finished.start();
        finished.finish();

        var cancelled = new LeafTask("t2", 1);
        cancelled.cancel();

        var running = new LeafTask("t3", 1);
        running.start();

        var pending = new LeafTask("t4", 1);

        var failed = new LeafTask("t5", 1);
        failed.fail();


        var jobId = new JobId("another job id");
        var mockedTasks = Stream.of(
            new UserTask("alice", jobId, pending),
            new UserTask("alice", jobId, running),
            new UserTask("alice", jobId, failed),
            new UserTask("alice", jobId, cancelled),
            new UserTask("alice", jobId, finished)
        );
        when(taskStore.query("alice")).thenReturn(mockedTasks);

        var actualProgress = operationsProcedureFacade.listProgress("", true);
        assertThat(actualProgress).map(ProgressResult::taskName).containsExactlyInAnyOrder(
            pending.description(),
            running.description(),
            cancelled.description(),
            failed.description(),
            finished.description()
        );
    }

    @Test
    void shouldQueryUserLog() {
        var userLogStore = mock(UserLogStore.class);
        var operationsApplications = OperationsApplications.create(
            null,
            RequestScopedDependencies.builder()
                .user(new User("current user", false))
                .userLogStore(userLogStore)
                .build()
        );
        var applicationsFacade = mock(ApplicationsFacade.class);
        when(applicationsFacade.operations()).thenReturn(operationsApplications);

        var operationsProcedureFacade = new LocalOperationsProcedureFacade(applicationsFacade);

        var expectedWarnings = Stream.of(
            new UserLogEntry(new LeafTask("lt", 42), "going once"),
            new UserLogEntry(new LeafTask("lt", 87), "going twice..."),
            new UserLogEntry(new LeafTask("lt", 23), "gone!")
        );
        when(userLogStore.query(new User("current user", false))).thenReturn(expectedWarnings);
        var actualWarnings = operationsProcedureFacade.queryUserLog(null);

        assertThat(actualWarnings).isSameAs(expectedWarnings);
    }
}
