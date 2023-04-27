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
package org.neo4j.gds.beta.node2vec;

import org.hamcrest.Matchers;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.ImmutableNodeProjection;
import org.neo4j.gds.ImmutablePropertyMappings;
import org.neo4j.gds.InvocationCountingTaskStore;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.ImmutableGraphLoader;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecModel;
import org.neo4j.gds.embeddings.node2vec.Node2VecMutateConfig;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.utils.StringJoining;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class Node2VecMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node1 {dummy:1})" +
        ", (b:Node1 {dummy:1})" +
        ", (c:Node2 {dummy:1})" +
        ", (d:Isolated {dummy:1})" +
        ", (e:Isolated {dummy:1})" +
        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(a)" +
        ", (a)-[:REL]->(c)" +
        ", (c)-[:REL]->(a)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(b)";

    @BeforeEach
    void loadProcedures() throws Exception {
        registerProcedures(
            Node2VecMutateProc.class,
            GraphWriteNodePropertiesProc.class,
            GraphProjectProc.class
        );

        runQuery("CALL gds.graph.project('graph', '*', '*')");
    }

    @AfterEach
    void removeAllLoadedGraphs() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testMutateFailsOnExistingToken() {

        var projectQuery = "CALL gds.graph.project('existingPropertyGraph', {N: {label: 'Node1', properties: 'dummy'}}, '*')";
        runQuery(projectQuery);

        var query = GdsCypher.call("existingPropertyGraph")
            .algo("gds.beta.node2vec")
            .mutateMode()
            .addParameter("embeddingDimension", 42)
            .addParameter("mutateProperty", "dummy")
            .addParameter("iterations", 1)
            .yields();

        assertThatExceptionOfType(QueryExecutionException.class)
            .isThrownBy(() -> runQuery(query))
            .withRootCauseExactlyInstanceOf(IllegalArgumentException.class)
            .withMessageContaining("Node property `dummy` already exists in the in-memory graph.");
    }

    @Test
    void mutation() {
        var graphBeforeMutation = findLoadedGraph("graph");
        assertThat(graphBeforeMutation.schema().nodeSchema().allProperties()).doesNotContain("testProp");

        runQuery("CALL gds.beta.node2vec.mutate('graph', {" +
                 "   embeddingDimension: 42, " +
                 "   mutateProperty: 'testProp', " +
                 "   iterations: 5" +
                 "})");

        var graphAfterMutation = findLoadedGraph("graph");
        assertThat(graphAfterMutation.schema().nodeSchema().allProperties()).contains("testProp");

        var mutatedProperty = graphAfterMutation.nodeProperties("testProp");
        graphAfterMutation.forEachNode(nodeId -> {
            assertThat(mutatedProperty.floatArrayValue(nodeId)).hasSize(42);
            return true;
        });
    }

    @Test
    void returnLossPerIteration() {
        var query = "CALL gds.beta.node2vec.mutate('graph', {" +
                    "   embeddingDimension: 42, " +
                    "   mutateProperty: 'testProp', " +
                    "   iterations: 5" +
                    "}) " +
                    "YIELD lossPerIteration";
        assertCypherResult(query, List.of(Map.of("lossPerIteration", Matchers.hasSize(5))));
    }


    @Test
    void shouldUnregisterTaskAfterComputation() {
        var taskStore = new InvocationCountingTaskStore();
        var graphProjectConfig = withNameAndRelationshipProjections(
            "g2"
        );
        GraphStoreCatalog.set(graphProjectConfig, graphLoader(graphProjectConfig).graphStore());
        applyOnProcedure(wccMutateProc -> {
            wccMutateProc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            var configMap = Map.<String, Object>of("mutateProperty", "embedding");

            var spec = new Node2VecMutateSpec() {
                @Override
                public ComputationResultConsumer<Node2Vec, Node2VecModel.Result, Node2VecMutateConfig, Stream<MutateResult>> computationResultConsumer() {
                    return (computationResultConsumer, executionContext) -> Stream.empty();
                }
            };
            new ProcedureExecutor<>(spec, wccMutateProc.executionContext()).compute("g2", configMap);
            new ProcedureExecutor<>(spec, wccMutateProc.executionContext()).compute("g2", configMap);

            assertThat(taskStore.query())
                .withFailMessage(() -> formatWithLocale(
                    "Expected no tasks to be open but found %s",
                    StringJoining.join(taskStore.query().map(TaskStore.UserTask::task).map(Task::description))
                )).isEmpty();
            assertThat(taskStore.registerTaskInvocations).isGreaterThan(1);
        });
    }

    @Test
    void shouldRegisterTaskWithCorrectJobId() {
        var taskStore = new InvocationCountingTaskStore();

        GraphProjectConfig graphProjectConfig = withNameAndRelationshipProjections(
            "g1"
        );
        applyOnProcedure(proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            GraphStore graphStore = graphLoader(graphProjectConfig).graphStore();
            GraphStoreCatalog.set(graphProjectConfig, graphStore);

            var someJobId = new JobId();
            Map<String, Object> configMap = Map.of(
                "jobId", someJobId,
                "mutateProperty", "embedding"
            );

            proc.mutate("g1", configMap);

            assertThat(taskStore.seenJobIds).containsExactly(someJobId);
        });
    }

    void applyOnProcedure(Consumer<Node2VecMutateProc> func) {
        TestProcedureRunner.applyOnProcedure(
            db,
            Node2VecMutateProc.class,
            func
        );
    }

    private GraphProjectFromStoreConfig withNameAndRelationshipProjections(
        String graphName
    ) {
        return ImmutableGraphProjectFromStoreConfig.of(
            getUsername(),
            graphName,
            NodeProjections.create(singletonMap(
                ALL_NODES,
                ImmutableNodeProjection.of(PROJECT_ALL, ImmutablePropertyMappings.of())
            )),
            RelationshipProjections.ALL
        );
    }

    @NotNull
    private GraphLoader graphLoader(GraphProjectConfig graphProjectConfig) {
        return ImmutableGraphLoader
            .builder()
            .context(ImmutableGraphLoaderContext.builder()
                .databaseId(DatabaseId.of(db))
                .dependencyResolver(GraphDatabaseApiProxy.dependencyResolver(db))
                .transactionContext(TestSupport.fullAccessTransaction(db))
                .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
                .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
                .log(Neo4jProxy.testLog())
                .build())
            .username("")
            .projectConfig(graphProjectConfig)
            .build();
    }
}
