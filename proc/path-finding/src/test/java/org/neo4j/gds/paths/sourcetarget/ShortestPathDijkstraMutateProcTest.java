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
package org.neo4j.gds.paths.sourcetarget;

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
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.paths.MutateResult;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraMutateConfig;
import org.neo4j.gds.utils.StringJoining;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ShortestPathDijkstraMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Label)" +
        ", (b:Label)" +
        ", (c:Label)" +
        ", (d:Label)" +
        ", (e:Label)" +
        ", (f:Label)" +
        ", (a)-[:T{w: 4.0D}]->(b)" +
        ", (a)-[:T{w: 2.0D}]->(c)" +
        ", (b)-[:T{w: 5.0D}]->(c)" +
        ", (b)-[:T{w: 10.0D}]->(d)" +
        ", (c)-[:T{w: 3.0D}]->(e)" +
        ", (d)-[:T{w: 11.0D}]->(f)" +
        ", (e)-[:T{w: 4.0D}]->(d)";

    public String expectedMutatedGraph() {
        return DB_CYPHER + ", (a)-[:PATH {w: 3.0D}]->(f)";
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            ShortestPathDijkstraMutateProc.class,
            GraphProjectProc.class
        );

        runQuery(GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Label")
            .withRelationshipType("T")
            .withRelationshipProperty("w")
            .yields());
    }

    @Test
    void shouldMutate() {

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.dijkstra")
            .mutateMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("targetNode", idFunction.of("f"))
            .addParameter("mutateRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 1L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actualGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "graph")
            .graphStore()
            .getUnion();
        var expectedGraph = TestSupport.fromGdl(expectedMutatedGraph());

        assertGraphEquals(expectedGraph, actualGraph);
    }

    @Test
    void testWeightedMutate() {

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.dijkstra")
            .mutateMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("targetNode", idFunction.of("f"))
            .addParameter("relationshipWeightProperty", "w")
            .addParameter("mutateRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 1L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actualGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "graph")
            .graphStore()
            .getUnion();
        var expected = TestSupport.fromGdl(DB_CYPHER + ", (a)-[:PATH {w: 20.0D}]->(f)");

        assertGraphEquals(expected, actualGraph);
    }

    @Test
    void shouldUnregisterTaskAfterComputation() {
        var taskStore = new InvocationCountingTaskStore();
        var graphProjectConfig = withNameAndRelationshipProjections(
            "g2"
        );
        GraphStoreCatalog.set(graphProjectConfig, graphLoader(graphProjectConfig).graphStore());
        applyOnProcedure(proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            var configMap = Map.<String, Object>of(
                "sourceNode",
                0,
                "targetNode",
                1,
                "mutateRelationshipType",
                "foo"
            );

            var spec = new ShortestPathDijkstraMutateSpec() {
                @Override
                public ComputationResultConsumer<Dijkstra, DijkstraResult, ShortestPathDijkstraMutateConfig, Stream<MutateResult>> computationResultConsumer() {
                    return (computationResultConsumer, executionContext) -> {
                        computationResultConsumer.result().get().pathSet();
                        return Stream.empty();
                    };
                }
            };
            new ProcedureExecutor<>(spec, proc.executionContext()).compute("g2", configMap);
            new ProcedureExecutor<>(spec, proc.executionContext()).compute("g2", configMap);

            assertThat(taskStore.query())
                .withFailMessage(() -> formatWithLocale(
                    "Expected no tasks to be open but found %s",
                    StringJoining.join(taskStore.query().map(TaskStore.UserTask::task).map(Task::description))
                )).isEmpty();
            assertThat(taskStore.registerTaskInvocations).isGreaterThan(1);
        });
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

    void applyOnProcedure(Consumer<ShortestPathDijkstraMutateProc> func) {
        TestProcedureRunner.applyOnProcedure(
            db,
            ShortestPathDijkstraMutateProc.class,
            func
        );
    }

}


