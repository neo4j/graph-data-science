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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.CommunityHelper;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.ImmutableNodeProjection;
import org.neo4j.gds.ImmutableNodeProjections;
import org.neo4j.gds.ImmutablePropertyMapping;
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
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
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
import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.config.GraphProjectFromStoreConfig.NODE_PROPERTIES_KEY;
import static org.neo4j.gds.config.GraphProjectFromStoreConfig.RELATIONSHIP_PROPERTIES_KEY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class WccStreamProcTest extends BaseProcTest {

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
    private static final long[][] EXPECTED_COMMUNITIES = {new long[]{0L, 1L, 2L, 3L, 4, 5, 6}, new long[]{7, 8}, new long[]{9}};

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            WccStreamProc.class,
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );
    }

    @AfterEach
    void removeAllLoadedGraphs() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testStreamWithDefaults() {
        var projectQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .loadEverything(Orientation.NATURAL)
            .yields();
        runQuery(projectQuery);
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .streamMode()
                .addParameter("minComponentSize", 1)
            .yields("nodeId", "componentId");

        long [] communities = new long[10];
        runQueryWithRowConsumer(query, row -> {
            int nodeId = row.getNumber("nodeId").intValue();
            long setId = row.getNumber("componentId").longValue();
            communities[nodeId] = setId;
        });

        CommunityHelper.assertCommunities(communities, EXPECTED_COMMUNITIES);
    }

    @Test
    void testStreamRunsOnLoadedGraph() {
        GraphProjectConfig graphProjectConfig = ImmutableGraphProjectFromStoreConfig
            .builder()
            .graphName("testGraph")
            .nodeProjections(NodeProjections.all())
            .relationshipProjections(RelationshipProjections.ALL)
            .build();

        GraphStoreCatalog.set(
            graphProjectConfig,
            graphLoader(graphProjectConfig).graphStore()
        );

        String query = GdsCypher.call("testGraph")
            .algo("wcc")
            .streamMode()
            .yields("nodeId", "componentId");

        long [] communities = new long[10];
        runQueryWithRowConsumer(query, row -> {
            int nodeId = row.getNumber("nodeId").intValue();
            long setId = row.getNumber("componentId").longValue();
            communities[nodeId] = setId;
        });

        CommunityHelper.assertCommunities(communities, EXPECTED_COMMUNITIES);
    }

    @Test
    void testStreamRunsOnLoadedGraphWithNodeLabelFilter() {
        clearDb();
        runQuery("CREATE (nX:Ignore {nodeId: 42}) " + DB_CYPHER + " CREATE (nX)-[:X]->(nA), (nA)-[:X]->(nX), (nX)-[:X]->(nE), (nE)-[:X]->(nX)");

        String graphCreateQuery = GdsCypher
            .call("nodeFilterGraph")
            .graphProject()
            .withNodeLabels("Label", "Label2", "Ignore")
            .withAnyRelationshipType()
            .yields();

        runQueryWithRowConsumer(graphCreateQuery, row -> {
            assertThat(row.getNumber("nodeCount"))
                .asInstanceOf(LONG)
                .isEqualTo(11L);
            assertThat(row.getNumber("relationshipCount"))
                .asInstanceOf(LONG)
                .isEqualTo(11L);
        });

        String query = GdsCypher.call("nodeFilterGraph")
            .algo("wcc")
            .streamMode()
            .addParameter("nodeLabels", Arrays.asList("Label", "Label2"))
            .yields("nodeId", "componentId");

        var actualCommunities = new HashSet<>();
        runQueryWithRowConsumer(query, row -> {
            actualCommunities.add(row.getNumber("componentId").longValue());
        });

        assertThat(actualCommunities).hasSize(3);
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
                Arguments.of(Map.of("minComponentSize", 1), new Long[]{0L, 0L, 0L, 0L, 0L, 0L, 0L, 7L, 7L, 9L}),
                Arguments.of(Map.of("minComponentSize", 3), new Long[]{0L, 0L, 0L, 0L, 0L, 0L, 0L})
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testStreamWithMinComponentSize(Map<String, Long> parameters, Long[] expectedCommunities) {
        var projectQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .loadEverything(Orientation.NATURAL)
            .yields();
        runQuery(projectQuery);
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
                .algo("wcc")
                .streamMode()
                .addAllParameters(parameters)
                .yields("nodeId", "componentId");

        Long [] communities = new Long[expectedCommunities.length];
        runQueryWithRowConsumer(query, row -> {
            int nodeId = row.getNumber("nodeId").intValue();
            long setId = row.getNumber("componentId").longValue();
            communities[nodeId] = setId;
        });

        assertThat(communities)
            .containsExactly(expectedCommunities);
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
            proc.compute(
                configMap,
                loadedGraphName
            ).result().get();
            proc.compute(
                configMap,
                loadedGraphName
            ).result().get();

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
            proc.compute(configMap, loadedGraphName);

            assertThat(taskStore.seenJobIds).containsExactly(someJobId);
        });
    }

    @Test
    void testRunOnEmptyGraph() {
        applyOnProcedure((proc) -> {
            var methods = Stream.concat(
                ProcedureMethodHelper.writeMethods(proc),
                ProcedureMethodHelper.streamMethods(proc)
            ).collect(Collectors.toList());

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
                    Map<String, Object> configMap = CypherMapWrapper.empty().toMap();

                    configMap.remove(NODE_PROPERTIES_KEY);
                    configMap.remove(RELATIONSHIP_PROPERTIES_KEY);
                    configMap.remove("relationshipWeightProperty");

                    if (configMap.containsKey("nodeWeightProperty")) {
                        var nodeProperty = String.valueOf(configMap.get("nodeWeightProperty"));
                        runQuery(
                            "CALL db.createProperty($prop)",
                            Map.of("prop", nodeProperty)
                        );
                        configMap.put(NODE_PROPERTIES_KEY, Map.ofEntries(ImmutablePropertyMapping
                            .builder()
                            .propertyKey(nodeProperty)
                            .defaultValue(DefaultValue.forDouble())
                            .build()
                            .toObject(false)
                        ));
                    }

                    try {
                        Stream<?> result = (Stream<?>) method.invoke(proc, graphName, configMap);
                        if (ProcedureMethodHelper.methodName(method).endsWith("stream")) {
                            assertEquals(0, result.count(), "Stream result should be empty.");
                        } else {
                            assertEquals(1, result.count());
                        }
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                });
            }
        });
    }

    private void applyOnProcedure(Consumer<WccStreamProc> func) {
        TestProcedureRunner.applyOnProcedure(
            db,
            WccStreamProc.class,
            func
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
