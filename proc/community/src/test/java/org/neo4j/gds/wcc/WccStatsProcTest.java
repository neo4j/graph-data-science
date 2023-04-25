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
package org.neo4j.gds.wcc;

import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.ImmutableNodeProjection;
import org.neo4j.gds.ImmutableNodeProjections;
import org.neo4j.gds.ImmutablePropertyMappings;
import org.neo4j.gds.InvocationCountingTaskStore;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.ProcedureMethodHelper;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.ImmutableGraphLoader;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.utils.StringJoining;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.assertj.ConditionFactory.containsAllEntriesOf;
import static org.neo4j.gds.assertj.ConditionFactory.containsExactlyInAnyOrderEntriesOf;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class WccStatsProcTest extends BaseProcTest {

    private static final String TEST_USERNAME = Username.EMPTY_USERNAME.username();

    @Neo4jGraph
    static final @Language("Cypher") String DB_CYPHER =
        "CREATE" +
        " (nA:Label {nodeId: 0, seedId: 42})" +
        ",(nB:Label {nodeId: 1, seedId: 42})" +
        ",(nC:Label {nodeId: 2, seedId: 42})" +
        ",(nD:Label {nodeId: 3, seedId: 42})" +
        ",(nE:Label2 {nodeId: 4})" +
        ",(nF:Label2 {nodeId: 5})" +
        ",(nG:Label2 {nodeId: 6})" +
        ",(nH:Label2 {nodeId: 7})" +
        ",(nI:Label2 {nodeId: 8})" +
        ",(nJ:Label2 {nodeId: 9})" +
        // {A, B, C, D}
        ",(nA)-[:TYPE]->(nB)" +
        ",(nB)-[:TYPE]->(nC)" +
        ",(nC)-[:TYPE]->(nD)" +
        ",(nD)-[:TYPE {cost:4.2}]->(nE)" + // threshold UF should split here
        // {E, F, G}
        ",(nE)-[:TYPE]->(nF)" +
        ",(nF)-[:TYPE]->(nG)" +
        // {H, I}
        ",(nH)-[:TYPE]->(nI)";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            WccStatsProc.class,
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );
    }

    @AfterEach
    void removeAllLoadedGraphs() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testRelationshipTypesEmpty() {
        runQuery("CALL gds.graph.project('g', '*', '*')");

        assertCypherResult("CALL gds.wcc.stats('g', {relationshipTypes: []}) YIELD componentCount", List.of(Map.of(
            "componentCount", 10L
        )));
    }

    @Test
    void yields() {
        var projectQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .loadEverything(Orientation.NATURAL)
            .yields();
        runQuery(projectQuery);
        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .statsMode()
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "componentCount", 3L,
            "componentDistribution", containsExactlyInAnyOrderEntriesOf(Map.of(
                "min", 1L,
                "max", 7L,
                "mean", 3.3333333333333335D,
                "p50", 2L,
                "p75", 7L,
                "p90", 7L,
                "p95", 7L,
                "p99", 7L,
                "p999", 7L
            )),
            "preProcessingMillis", greaterThanOrEqualTo(0L),
            "computeMillis", greaterThanOrEqualTo(0L),
            "postProcessingMillis", greaterThanOrEqualTo(0L),
            "configuration", containsAllEntriesOf(MapUtil.map(
                "consecutiveIds", false,
                "threshold", 0D,
                "seedProperty", null
            ))
        )));
    }

    @Test
    void zeroComponentsInEmptyGraph() {
        runQuery("CALL db.createLabel('VeryTemp')");
        runQuery("CALL db.createRelationshipType('VERY_TEMP')");

        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("VeryTemp")
            .withRelationshipType("VERY_TEMP")
            .yields();
        runQuery(createQuery);

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .statsMode()
            .yields("componentCount");

        assertCypherResult(query, List.of(Map.of("componentCount", 0L)));
    }

    @Test
    void statsShouldNotHaveWriteProperties() {
        var projectQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .loadEverything(Orientation.NATURAL)
            .yields();
        runQuery(projectQuery);
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .statsMode()
            .yields();

        List<String> forbiddenResultColumns = Arrays.asList(
            "writeMillis",
            "nodePropertiesWritten",
            "relationshipPropertiesWritten"
        );
        List<String> forbiddenConfigKeys = Collections.singletonList("writeProperty");
        runQueryWithResultConsumer(query, result -> {
            List<String> badResultColumns = result.columns()
                .stream()
                .filter(forbiddenResultColumns::contains)
                .collect(Collectors.toList());
            assertEquals(Collections.emptyList(), badResultColumns);
            assertTrue(result.hasNext(), "Result must not be empty.");
            Map<String, Object> config = (Map<String, Object>) result.next().get("configuration");
            List<String> badConfigKeys = config.keySet()
                .stream()
                .filter(forbiddenConfigKeys::contains)
                .collect(Collectors.toList());
            assertEquals(Collections.emptyList(), badConfigKeys);
        });
    }

    @Test
    void shouldUnregisterTaskAfterComputation() {
        var taskStore = new InvocationCountingTaskStore();

        var loadedGraphName = "loadedGraph";
        var graphProjectConfig = withNameAndRelationshipProjections(
            loadedGraphName,
            RelationshipProjections.ALL
        );

        GraphStoreCatalog.set(graphProjectConfig, graphLoader(graphProjectConfig).graphStore());

        applyOnProcedure(proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            var configMap = CypherMapWrapper.empty().toMap();
            proc.stats(
                loadedGraphName,
                configMap
            ).count();
            proc.stats(
                loadedGraphName,
                configMap
            ).count();

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

        String loadedGraphName = "loadedGraph";
        GraphProjectConfig graphProjectConfig = withNameAndRelationshipProjections(
            loadedGraphName,
            RelationshipProjections.ALL
        );
        applyOnProcedure(proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            GraphStore graphStore = graphLoader(graphProjectConfig).graphStore();
            GraphStoreCatalog.set(graphProjectConfig, graphStore);

            var someJobId = new JobId();
            Map<String, Object> mapWithJobId = Map.of("jobId", someJobId);

            Map<String, Object> configMap = CypherMapWrapper.create(mapWithJobId).toMap();
            proc.stats(loadedGraphName, configMap);

            assertThat(taskStore.seenJobIds).containsExactly(someJobId);
        });
    }

    @Test
    void testRunOnEmptyGraph() {
        applyOnProcedure((proc) -> {
            var methods = ProcedureMethodHelper.statsMethods(proc).collect(Collectors.toList());

            if (!methods.isEmpty()) {
                // Create a dummy node with label "X" so that "X" is a valid label to put use for property mappings later
                runQuery("CALL db.createLabel('X')");
                runQuery("MATCH (n) DETACH DELETE n");
                GraphStoreCatalog.removeAllLoadedGraphs();

                var graphName = "graph";
                var graphProjectConfig = ImmutableGraphProjectFromStoreConfig.of(
                    TEST_USERNAME,
                    graphName,
                    ImmutableNodeProjections.of(
                        Map.of(NodeLabel.of("X"), ImmutableNodeProjection.of("X", ImmutablePropertyMappings.of()))
                    ),
                    RelationshipProjections.ALL
                );
                var graphStore = graphLoader(graphProjectConfig).graphStore();
                GraphStoreCatalog.set(graphProjectConfig, graphStore);
                methods.forEach(method -> {
                    try {
                        Stream<?> result = (Stream<?>) method.invoke(proc, graphName, Map.of());
                        assertEquals(1, result.count());
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                });
            }
        });
    }

    private void applyOnProcedure(Consumer<WccStatsProc> func) {
        TestProcedureRunner.applyOnProcedure(
            db,
            WccStatsProc.class,
            func::accept
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

    private GraphProjectFromStoreConfig withNameAndRelationshipProjections(
        String graphName,
        RelationshipProjections rels
    ) {
        return ImmutableGraphProjectFromStoreConfig.of(
            TEST_USERNAME,
            graphName,
            NodeProjections.create(singletonMap(
                ALL_NODES,
                ImmutableNodeProjection.of(PROJECT_ALL, ImmutablePropertyMappings.of())
            )),
            rels
        );
    }
}
