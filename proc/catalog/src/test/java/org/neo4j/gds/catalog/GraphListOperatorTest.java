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
package org.neo4j.gds.catalog;

import org.assertj.core.api.AbstractBooleanAssert;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.ImmutableRelationshipProjections;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.EmptyDependencyResolver;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.TerminationMonitor;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.executor.ImmutableExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.gds.assertj.AssertionsHelper.booleanAssertConsumer;
import static org.neo4j.gds.assertj.AssertionsHelper.creationTimeAssertConsumer;
import static org.neo4j.gds.assertj.AssertionsHelper.intAssertConsumer;
import static org.neo4j.gds.assertj.AssertionsHelper.stringObjectMapAssertFactory;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
class GraphListOperatorTest {
    static String GRAPH_QUERY = "CREATE (:A {foo: 1})-[:REL {bar: 2}]->(:A)";

    @GdlGraph(idOffset = 42)
    private static final String DB_CYPHER = GRAPH_QUERY;

    @Inject
    private GraphStore graphStore;

    @GdlGraph(idOffset = 42, graphNamePrefix = "noProperties")
    private static final String NO_PROPERTIES_DB_CYPHER = "CREATE (:A)-[:REL]->(:A)";

    @Inject
    private GraphStore noPropertiesGraphStore;
    @GdlGraph(idOffset = 42, orientation = Orientation.REVERSE, graphNamePrefix = "reverse")
    private static final String REVERSE_DB_CYPHER = GRAPH_QUERY;

    @Inject
    private GraphStore reverseGraphStore;

    @GdlGraph(idOffset = 42, orientation = Orientation.UNDIRECTED, graphNamePrefix = "undirected")
    private static final String UNDIRECTED_DB_CYPHER = GRAPH_QUERY;

    @Inject
    private GraphStore undirectedGraphStore;


    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void listAllAvailableGraphsForUser() {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("Alice", "aliceGraph"), graphStore);
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("Bob", "bobGraph"), graphStore);

        var executionContextAlice = executionContextBuilder("Alice")
            .build();
        var executionContextBob = executionContextBuilder("Bob")
            .build();

        var resultAlice=GraphListOperator.list(GraphListOperator.NO_VALUE,executionContextAlice);
        var resultBob=GraphListOperator.list(GraphListOperator.NO_VALUE,executionContextBob);
        assertThat(resultAlice.findFirst().get().graphName).isEqualTo("aliceGraph");
        assertThat(resultBob.findFirst().get().graphName).isEqualTo("bobGraph");

    }

    @Test
    void shouldHaveCreationTimeField() {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("Alice", "aliceGraph"), graphStore);
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("Bob", "bobGraph"), graphStore);
        var executionContextAlice = executionContextBuilder("Alice")
            .build();
        var executionContextBob = executionContextBuilder("Bob")
            .build();
        AtomicReference<String> creationTimeAlice = new AtomicReference<>();
        AtomicReference<String> creationTimeBob = new AtomicReference<>();

        var resultAlice = GraphListOperator.list(GraphListOperator.NO_VALUE, executionContextAlice);
        var resultBob = GraphListOperator.list(GraphListOperator.NO_VALUE, executionContextBob);
        creationTimeAlice.set(formatCreationTime(resultAlice.findFirst().get().creationTime));
        creationTimeAlice.set(formatCreationTime(resultBob.findFirst().get().creationTime));

        assertNotEquals(creationTimeAlice.get(), creationTimeBob.get());
    }

    @ParameterizedTest(name = "name argument: ''{0}''")
    @ValueSource(strings = {"foobar", "aa"})
    void returnEmptyStreamWhenNoGraphMatchesTheFilterArgument(String argument) {
        String[] names = {"a", "b", "c"};
        for (String name : names) {
            GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", name), graphStore);
        }
        var executionContext = executionContextBuilder("user")
            .build();
        var result = GraphListOperator.list(argument, executionContext);
        assertThat(result.count()).isEqualTo(0L);

    }

    @Test
    void filterOnExactMatchUsingTheFirstArgument() {
        String[] names = {"b", "bb", "ab", "ba", "B", "ʙ"};
        for (String name : names) {
            GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", name), graphStore);
        }
        var executionContext = executionContextBuilder("user").build();

        var name = names[0];
        var result = GraphListOperator.list(name, executionContext).collect(Collectors.toList());
        assertThat(result.size()).isEqualTo(1L);
        Assertions.assertThat(result.get(0).graphName).isEqualTo(name);
    }

    @Test
    void returnEmptyStreamWhenNoGraphsAreLoaded() {
        var executionContextAlice = executionContextBuilder("Alice")
            .build();
        var resultAlice = GraphListOperator.list(GraphListOperator.NO_VALUE, executionContextAlice);
        assertThat(resultAlice.count()).isEqualTo(0L);
    }

    @Test
    void reverseProjectionForListing() {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "graph"), reverseGraphStore);

        var executionContext = executionContextBuilder("user")
            .build();
        var result = GraphListOperator.list(GraphListOperator.NO_VALUE, executionContext);
        assertThat(result.findFirst().get().nodeCount).isEqualTo(2L);
    }

    @Test
    void degreeDistributionComputationIsOptOut() {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "graph"), graphStore);
        var executionContext = executionContextBuilder(
            "user",
            Set.of("graphName", "nodeCount", "relationshipCount")
        ).build();
        var result = GraphListOperator.list(GraphListOperator.NO_VALUE, executionContext).findFirst().get();

        assertThat(result.nodeCount).isEqualTo(2L);
        assertThat(result.relationshipCount).isEqualTo(1L);
        assertThat(result.graphName).isEqualTo("graph");
        assertThat(result.degreeDistribution).isNull();

    }

    @Test
    void calculateDegreeDistributionForOutgoingRelationshipsWhenAskedTo() {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "graph"), graphStore);
        var executionContext = executionContextBuilder(
            "user",
            Set.of("degreeDistribution")
        ).build();
        var result = GraphListOperator.list(GraphListOperator.NO_VALUE, executionContext).findFirst().get();

        assertThat(result.nodeCount).isEqualTo(2L);
        assertThat(result.relationshipCount).isEqualTo(1L);
        assertThat(result.graphName).isEqualTo("graph");
        assertThat(result.degreeDistribution).isNotNull()
            .asInstanceOf(MAP).containsAllEntriesOf(
                Map.of("min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                )
            );
    }

    @Test
    void calculateDegreeDistributionForIncomingRelationshipsWhenAskedTo() {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "graph"), reverseGraphStore);
        var executionContext = executionContextBuilder(
            "user",
            Set.of("degreeDistribution")
        ).build();
        var result = GraphListOperator.list(GraphListOperator.NO_VALUE, executionContext).findFirst().get();

        assertThat(result.nodeCount).isEqualTo(2L);
        assertThat(result.relationshipCount).isEqualTo(1L);
        assertThat(result.graphName).isEqualTo("graph");
        assertThat(result.degreeDistribution).isNotNull()
            .asInstanceOf(MAP).containsAllEntriesOf(
                Map.of("min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                )
            );
    }

    @Test
    void calculateDegreeDistributionForUndirectedNodesWhenAskedTo() {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "graph"), undirectedGraphStore);
        var executionContext = executionContextBuilder(
            "user",
            Set.of("degreeDistribution")
        ).build();
        var result = GraphListOperator.list(GraphListOperator.NO_VALUE, executionContext).findFirst().get();

        assertThat(result.nodeCount).isEqualTo(2L);
        assertThat(result.relationshipCount).isEqualTo(2L);
        assertThat(result.graphName).isEqualTo("graph");
        assertThat(result.degreeDistribution).isNotNull()
            .asInstanceOf(MAP).containsAllEntriesOf(
                Map.of(
                    "min", 1L,
                    "mean", 1.0,
                    "max", 1L,
                    "p50", 1L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                )
            );
    }

    @ParameterizedTest(name = "name argument: {0}")
    @ValueSource(strings = {"", "null"})
    void listAllGraphsWhenCalledWithoutArgumentOrAnEmptyArgument(String argument) {
        String[] names = {"a", "b", "c"};
        for (String name : names) {
            GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", name), graphStore);
        }
        var executionContext = executionContextBuilder("user").build();

        var actualNames = GraphListOperator.list(GraphListOperator.NO_VALUE, executionContext)
            .map(v -> v.graphName)
            .collect(
                Collectors.toList());

        Assertions.assertThat(actualNames).containsExactlyInAnyOrder(names);
    }

    @Test
    void calculateActualMemoryUsage() {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "graph"), graphStore);
        var executionContext = executionContextBuilder(
            "user",
            Set.of("memoryUsage", "sizeInBytes")
        ).build();
        var result = GraphListOperator.list(GraphListOperator.NO_VALUE, executionContext).findFirst().get();
        assertThat(result.memoryUsage).isInstanceOf(String.class);
        assertThat(result.sizeInBytes).isInstanceOf(Long.class);

    }

    @Test
    void listASingleLabelRelationshipTypeProjection() {
        String name = "name";
        GraphStoreCatalog.set(
            GraphProjectFromStoreConfig.of(
                "user",
                name,
                NodeProjections.single(NodeLabel.of("A"), NodeProjection.of("A")),
                ImmutableRelationshipProjections.of(Map.of(
                    RelationshipType.of("REL"),
                    RelationshipProjection.of("REL", Orientation.NATURAL)
                )), CypherMapWrapper.empty()
            ),
            noPropertiesGraphStore
        );
        var executionContext = executionContextBuilder("user", Set.of("degreeDistribution")).build();

        var result = GraphListOperator.list(GraphListOperator.NO_VALUE, executionContext).findFirst().get();

        assertThat(result.graphName).isEqualTo(name);
        assertThat(result.database).isEqualTo("gdl");
        assertThat(result.nodeCount).isEqualTo(2L);
        assertThat(result.relationshipCount).isEqualTo(1L);
        assertThat(result.density).isEqualTo(0.5D);
        assertThat(result.degreeDistribution)
            .asInstanceOf(MAP)
            .containsAllEntriesOf(
                Map.of(
                    "min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                ));
        assertThat(result.creationTime).isInstanceOf(ZonedDateTime.class);
        assertThat(result.modificationTime).isInstanceOf(ZonedDateTime.class);
        assertThat(result.memoryUsage).isInstanceOf(String.class);
        assertThat(result.sizeInBytes).isInstanceOf(Long.class);
        assertThat(result.schema).asInstanceOf(MAP).containsExactlyInAnyOrderEntriesOf(Map.of("nodes", map("A", map()),
            "relationships", map("REL", map()),
            "graphProperties", map()
        ));
        assertThat(result.schemaWithOrientation).asInstanceOf(MAP).containsExactlyInAnyOrderEntriesOf(Map.of(
            "nodes",
            map("A", map()),
            "relationships",
            map("REL", map("direction", "DIRECTED", "properties", map())),
            "graphProperties",
            map()
        ));
        assertThat(result.configuration)
            .asInstanceOf(stringObjectMapAssertFactory())
            .hasSize(10)
            .containsEntry(
                "nodeProjection", map(
                    "A", map(
                        "label", "A",
                        "properties", emptyMap()
                    )
                )
            )
            .containsEntry(
                "relationshipProjection", map(
                    "REL", map(
                        "type", "REL",
                        "orientation", "NATURAL",
                        "aggregation", "DEFAULT",
                        "indexInverse", false,
                        "properties", emptyMap()
                    )
                )
            )
            .containsEntry("relationshipProperties", emptyMap())
            .containsEntry("nodeProperties", emptyMap())
            .hasEntrySatisfying("creationTime", creationTimeAssertConsumer())
            .hasEntrySatisfying(
                "validateRelationships",
                booleanAssertConsumer(AbstractBooleanAssert::isFalse)
            )
            .hasEntrySatisfying(
                "readConcurrency",
                intAssertConsumer(readConcurrency -> readConcurrency.isEqualTo(4))
            )
            .hasEntrySatisfying("sudo", booleanAssertConsumer(AbstractBooleanAssert::isFalse))
            .hasEntrySatisfying("logProgress", booleanAssertConsumer(AbstractBooleanAssert::isTrue))
            .doesNotContainKeys(
                GraphProjectConfig.NODE_COUNT_KEY,
                GraphProjectConfig.RELATIONSHIP_COUNT_KEY,
                "username"
            );
    }


    private ImmutableExecutionContext.Builder executionContextBuilder(String username) {
        return executionContextBuilder(username, Set.of());
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.catalog.GraphProjectProcTest#invalidGraphNames")
    void failsOnInvalidGraphName(String invalidName) {

        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "graph"), graphStore);
        var executionContext = executionContextBuilder(
            "user",
            Set.of("memoryUsage", "sizeInBytes")
        ).build();

        if (invalidName != null) { // null is not a valid name, but we use it to mean 'list all'
            assertThatThrownBy(() -> GraphListOperator.list(invalidName, executionContext)
                .findAny()).hasMessageContaining(
                formatWithLocale("`graphName` can not be null or blank, but it was `%s`", invalidName));
        } else {
            assertThatNoException().isThrownBy(() -> GraphListOperator.list(invalidName, executionContext));
        }
    }

    private ImmutableExecutionContext.Builder executionContextBuilder(String username, Set<String> returnFields) {
        return ImmutableExecutionContext
            .builder()
            .databaseId(graphStore.databaseId())
            .dependencyResolver(EmptyDependencyResolver.INSTANCE)
            .returnColumns(fieldName -> returnFields.contains(fieldName))
            .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
            .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
            .username(username)
            .terminationMonitor(TerminationMonitor.EMPTY)
            .closeableResourceRegistry(CloseableResourceRegistry.EMPTY)
            .algorithmMetaDataSetter(AlgorithmMetaDataSetter.EMPTY)
            .nodeLookup(NodeLookup.EMPTY)
            .log(Neo4jProxy.testLog())
            .isGdsAdmin(false);
    }
    private String formatCreationTime(ZonedDateTime zonedDateTime) {
        return ISO_LOCAL_DATE_TIME.format(zonedDateTime);
    }

}
