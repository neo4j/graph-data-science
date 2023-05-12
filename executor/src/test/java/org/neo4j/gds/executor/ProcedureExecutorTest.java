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
import org.neo4j.gds.ProcedureCallContextReturnColumns;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.EmptyDependencyResolver;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.TerminationMonitor;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.GlobalTaskStore;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.utils.StringJoining;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;


@GdlExtension
class ProcedureExecutorTest {

    @GdlGraph
    public static final String DB_CYPHER =
        "CREATE " +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (a)-[:TYPE]->(b)";

    @Inject
    private GraphStore graphStore;

    @BeforeEach
    void setUp() {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("", "graph"), graphStore);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldRegisterTaskWithCorrectJobId() {
        // Arrange
        var taskStore = new InvocationCountingTaskStore();
        var executor = new ProcedureExecutor<>(new TestMutateSpec(), executionContext(taskStore));
        var someJobId = new JobId();
        var configuration = Map.of(
            "mutateProperty", "mutateProperty",
            "sudo", true,
            "jobId", someJobId
        );

        // Act
        executor.compute("graph", configuration);

        // Assert
        assertThat(taskStore.seenJobIds).containsExactly(someJobId);
    }

    @Test
    void shouldUnregisterTaskAfterComputation() {
        // Arrange
        var taskStore = new InvocationCountingTaskStore();
        var executor1 = new ProcedureExecutor<>(new TestMutateSpec(), executionContext(taskStore));
        var executor2 = new ProcedureExecutor<>(new TestMutateSpec(), executionContext(taskStore));
        var configuration = Map.<String, Object>of(
            "mutateProperty", "mutateProperty",
            "sudo", true
        );

        // Act
        executor1.compute("graph", configuration);
        executor2.compute("graph", configuration);

        // Assert
        assertThat(taskStore.query())
            .withFailMessage(() -> formatWithLocale(
                "Expected no tasks to be open but found %s",
                StringJoining.join(taskStore.query().map(TaskStore.UserTask::task).map(Task::description))
            )).isEmpty();

        assertThat(taskStore.registerTaskInvocations)
            .as("We created two tasks => two tasks must have been registered")
            .isEqualTo(2);
    }

    private ExecutionContext executionContext(TaskStore taskStore) {
        return ImmutableExecutionContext
            .builder()
            .databaseId(graphStore.databaseId())
            .log(Neo4jProxy.testLog())
            .returnColumns(ProcedureCallContextReturnColumns.EMPTY)
            .taskRegistryFactory(jobId -> new TaskRegistry("", taskStore, jobId))
            .username("")
            .terminationMonitor(TerminationMonitor.EMPTY)
            .isGdsAdmin(true)
            .dependencyResolver(EmptyDependencyResolver.INSTANCE)
            .modelCatalog(ModelCatalog.EMPTY)
            .closeableResourceRegistry(CloseableResourceRegistry.EMPTY)
            .algorithmMetaDataSetter(AlgorithmMetaDataSetter.EMPTY)
            .nodeLookup(NodeLookup.EMPTY)
            .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
            .build();
    }

    private static class InvocationCountingTaskStore extends GlobalTaskStore {
        int registerTaskInvocations;
        Set<JobId> seenJobIds = new HashSet<>();

        @Override
        public void store(String username, JobId jobId, Task task) {
            super.store(username, jobId, task);
            registerTaskInvocations++;
            seenJobIds.add(jobId);
        }
    }

}
