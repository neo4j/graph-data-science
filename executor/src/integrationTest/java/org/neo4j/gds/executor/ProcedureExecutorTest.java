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
package org.neo4j.gds.executor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.PerDatabaseTaskStore;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.TaskStoreListener;
import org.neo4j.gds.core.utils.progress.UserTask;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.termination.TerminationMonitor;
import org.neo4j.gds.utils.StringJoining;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;


@GdlExtension
class ProcedureExecutorTest {

    @GdlGraph
    public static final String DB_CYPHER = "CREATE " +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (a)-[:TYPE]->(b)";

    @Inject
    private GraphStore graphStore;

    @BeforeEach
    void setUp() {
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("", "graph"), graphStore);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldRegisterTaskWithCorrectJobId() {
        // Arrange
        var jobIdTracker = new JobIdTracker();
        var taskStore = new PerDatabaseTaskStore();
        taskStore.addListener(jobIdTracker);
        var executor = new ProcedureExecutor<>(new TestMutateSpec(), executionContext(taskStore));
        var someJobId = new JobId();
        var configuration = Map.of(
            "mutateProperty",
            "mutateProperty",
            "sudo",
            true,
            "jobId",
            someJobId
        );

        // Act
        executor.compute("graph", configuration);

        // Assert
        assertThat(jobIdTracker.seenJobIds).containsExactly(someJobId);
    }

    @Test
    void shouldUnregisterTaskAfterComputation() {
        // Arrange
        var invocationCounter = new TaskCreatedCounter();
        var taskStore = new PerDatabaseTaskStore();
        taskStore.addListener(invocationCounter);
        var executor1 = new ProcedureExecutor<>(new TestMutateSpec(), executionContext(taskStore));
        var executor2 = new ProcedureExecutor<>(new TestMutateSpec(), executionContext(taskStore));
        var configuration = Map.<String, Object>of(
            "mutateProperty",
            "mutateProperty",
            "sudo",
            true
        );

        // Act
        executor1.compute("graph", configuration);
        executor2.compute("graph", configuration);

        // Assert
        assertThat(taskStore.query())
            .withFailMessage(
                () -> formatWithLocale(
                    "Expected no tasks to be open but found %s",
                    StringJoining.join(taskStore.query().map(UserTask::task).map(Task::description))
                )
            )
            .isEmpty();

        assertThat(invocationCounter.registerTaskInvocations)
            .as("We created two tasks => two tasks must have been registered")
            .isEqualTo(2);
    }

    private ExecutionContext executionContext(TaskStore taskStore) {
        return ImmutableExecutionContext
            .builder()
            .databaseId(graphStore.databaseInfo().databaseId())
            .log(Log.noOpLog())
            .returnColumns(ProcedureReturnColumns.EMPTY)
            .taskRegistryFactory(jobId -> new TaskRegistry("", taskStore, jobId))
            .username("")
            .terminationMonitor(TerminationMonitor.EMPTY)
            .isGdsAdmin(true)
            .dependencyResolver(new DependencyResolver() {
                @Override
                public <T> T resolveDependency(Class<T> type, SelectionStrategy selector) {
                    return null;
                }

                @Override
                public boolean containsDependency(Class<?> type) {
                    return false;
                }
            })
            .modelCatalog(ModelCatalog.EMPTY)
            .closeableResourceRegistry(CloseableResourceRegistry.EMPTY)
            .nodeLookup(NodeLookup.EMPTY)
            .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
            .metrics(Metrics.DISABLED)
            .build();
    }

    private static class TaskCreatedCounter implements TaskStoreListener {
        int registerTaskInvocations;

        @Override
        public void onTaskAdded(UserTask userTask) {
            registerTaskInvocations++;
        }

        @Override
        public void onTaskRemoved(UserTask userTask) {

        }
    }

    private static class JobIdTracker implements TaskStoreListener {
        Set<JobId> seenJobIds = new HashSet<>();

        @Override
        public void onTaskAdded(UserTask userTask) {
            seenJobIds.add(userTask.jobId());
        }

        @Override
        public void onTaskRemoved(UserTask userTask) {

        }
    }

}
