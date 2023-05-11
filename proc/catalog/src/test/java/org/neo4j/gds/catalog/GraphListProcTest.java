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
import org.assertj.core.api.Condition;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.beta.generator.GraphGenerateProc;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.projection.CypherAggregation;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.assertj.AssertionsHelper.booleanAssertConsumer;
import static org.neo4j.gds.assertj.AssertionsHelper.creationTimeAssertConsumer;
import static org.neo4j.gds.assertj.AssertionsHelper.intAssertConsumer;
import static org.neo4j.gds.assertj.AssertionsHelper.stringAssertConsumer;
import static org.neo4j.gds.assertj.AssertionsHelper.stringObjectMapAssertFactory;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.register;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.ALL_RELATIONSHIPS_QUERY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class GraphListProcTest extends BaseProcTest {

    private static final String DB_CYPHER = "CREATE (:A {foo: 1})-[:REL {bar: 2}]->(:A)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,

            GraphGenerateProc.class,
            GraphListProc.class
        );
        register(db, Neo4jProxy.callableUserAggregationFunction(CypherAggregation.newInstance()));
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void listASingleLabelRelationshipTypeProjection() {
        String name = "name";
        runQuery("CALL gds.graph.project($name, 'A', 'REL')", map("name", name));

        assertCypherResult("CALL gds.graph.list()", singletonList(
            map(
                "graphName", name,
                "database", "neo4j",
                "configuration",
                new Condition<>(config -> {
                    assertThat(config)
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

                    return true;
                }, "Assert native `configuration` map"),
                "schema", map(
                    "nodes", map("A", map()),
                    "relationships", map("REL", map()),
                    "graphProperties", map()
                ),
                "schemaWithOrientation", map(
                    "nodes", map("A", map()),
                    "relationships", map("REL", map("direction", "DIRECTED", "properties", map())),
                    "graphProperties", map()
                ),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "density", 0.5D,
                "degreeDistribution", map(
                    "min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                ),
                "creationTime", isA(ZonedDateTime.class),
                "modificationTime", isA(ZonedDateTime.class),
                "memoryUsage", instanceOf(String.class),
                "sizeInBytes", instanceOf(Long.class)
            )
        ));
    }

    @Test
    void shouldCacheDegreeDistribution() {
        var name = "name";
        var generateQuery = "CALL gds.beta.graph.generate($name, 500000, 5)";
        runQuery(
            generateQuery,
            map("name", name)
        );

        assertFalse(graphIsCached());
        runQuery("CALL gds.graph.list() YIELD degreeDistribution");
        assertTrue(graphIsCached());
        runQuery("CALL gds.graph.list() YIELD degreeDistribution");
        assertTrue(graphIsCached());
        runQuery("CALL gds.graph.list() YIELD degreeDistribution");
        assertTrue(graphIsCached());
    }

    private boolean graphIsCached() {
        return GraphStoreCatalog
            .getDegreeDistribution(getUsername(), DatabaseId.of(db), "name")
            .isPresent();
    }


    @Test
    void listGeneratedGraph() {
        var name = "name";
        var generateQuery = "CALL gds.beta.graph.generate($name, 10, 5)";
        runQuery(
            generateQuery,
            map("name", name)
        );

        assertCypherResult("CALL gds.graph.list()", singletonList(
            map(
                "graphName", name,
                "database", "neo4j",
                "configuration", new Condition<>(config -> {
                    assertThat(config)
                        .asInstanceOf(stringObjectMapAssertFactory())
                        .hasSize(12)
                        .containsEntry("nodeProjections", map(
                            "10_Nodes", map(
                                "label", "10_Nodes",
                                "properties", emptyMap()
                            )
                        ))
                        .containsEntry("relationshipProjections", map(
                            "REL", map(
                                "type", "REL",
                                "orientation", "NATURAL",
                                "aggregation", "NONE",
                                "indexInverse", false,
                                "properties", emptyMap()
                            )
                        ))
                        .hasEntrySatisfying(
                            "orientation",
                            stringAssertConsumer(orientation -> orientation.isEqualTo("NATURAL"))
                        )
                        .hasEntrySatisfying(
                            "relationshipProperty",
                            relationshipProperty -> assertThat(relationshipProperty)
                                .asInstanceOf(stringObjectMapAssertFactory())
                                .isEmpty()
                        )
                        .hasEntrySatisfying("creationTime", creationTimeAssertConsumer())
                        .hasEntrySatisfying(
                            "aggregation",
                            stringAssertConsumer(aggregation -> aggregation.isEqualTo("NONE"))
                        )
                        .hasEntrySatisfying("allowSelfLoops", booleanAssertConsumer(AbstractBooleanAssert::isFalse))
                        .hasEntrySatisfying("sudo", booleanAssertConsumer(AbstractBooleanAssert::isFalse))
                        .hasEntrySatisfying("logProgress", booleanAssertConsumer(AbstractBooleanAssert::isTrue))
                        .hasEntrySatisfying(
                            "relationshipDistribution",
                            stringAssertConsumer(relationshipDistribution -> relationshipDistribution.isEqualTo(
                                "UNIFORM"))
                        )
                        .hasEntrySatisfying(
                            "relationshipSeed",
                            relationshipSeed -> assertThat(relationshipSeed).isNull()
                        )
                        .doesNotContainKeys(
                            "readConcurrency",
                            "username",
                            "validateRelationships",
                            "relationshipCount",
                            "nodeCount"
                        );
                    return true;
                }, "Assert generated `configuration` map"),
                "schema", map(
                    "nodes", map("__ALL__", map()),
                    "relationships", map("REL", map()),
                    "graphProperties", map()
                ),
                "schemaWithOrientation", map(
                    "nodes", map("__ALL__", map()),
                    "relationships", map("REL", map("direction", "DIRECTED", "properties", map())),
                    "graphProperties", map()
                ),
                "nodeCount", 10L,
                "relationshipCount", 50L,
                "degreeDistribution", map(
                    "min", 5L,
                    "mean", 5.0D,
                    "max", 5L,
                    "p50", 5L,
                    "p75", 5L,
                    "p90", 5L,
                    "p95", 5L,
                    "p99", 5L,
                    "p999", 5L
                ),
                "creationTime", isA(ZonedDateTime.class),
                "modificationTime", isA(ZonedDateTime.class),
                "memoryUsage", instanceOf(String.class),
                "sizeInBytes", instanceOf(Long.class),
                "density", new Condition<>(Double::isFinite, "a finite double")
            )
        ));
    }

    @Test
    void listASingleLabelRelationshipTypeProjectionWithProperties() {
        String name = "name";
        runQuery(
            "CALL gds.graph.project($name, 'A', 'REL', {nodeProperties: 'foo', relationshipProperties: 'bar'})",
            map("name", name)
        );

        assertCypherResult("CALL gds.graph.list() YIELD schema", singletonList(
            map(
                "schema", map(
                    "nodes", map("A", map("foo", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)")),
                    "relationships", map("REL", map("bar", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)")),
                    "graphProperties", map()
                )
            )
        ));
    }

    @Test
    void listCypherProjection() {
        String name = "name";
        runQuery(
            "CALL gds.graph.project.cypher($name, $nodeQuery, $relationshipQuery)",
            map("name", name, "nodeQuery", ALL_NODES_QUERY, "relationshipQuery", ALL_RELATIONSHIPS_QUERY)
        );

        assertCypherResult("CALL gds.graph.list()", singletonList(
            map(
                "graphName", name,
                "database", "neo4j",
                "schema", map(
                    "nodes", map(ALL_NODES.name, map()),
                    "relationships", map(ALL_RELATIONSHIPS.name, map()),
                    "graphProperties", map()
                ),
                "schemaWithOrientation", map(
                    "nodes", map(ALL_NODES.name, map()),
                    "relationships", map(ALL_RELATIONSHIPS.name, map("direction", "DIRECTED", "properties", map())),
                    "graphProperties", map()
                ),
                "configuration", new Condition<>(config -> {
                    assertThat(config)
                        .asInstanceOf(stringObjectMapAssertFactory())
                        .hasSize(9)
                        .hasEntrySatisfying(
                            "relationshipQuery",
                            stringAssertConsumer(relationshipQuery -> relationshipQuery.isEqualTo(
                                ALL_RELATIONSHIPS_QUERY))
                        )
                        .hasEntrySatisfying("creationTime", creationTimeAssertConsumer())
                        .hasEntrySatisfying(
                            "validateRelationships",
                            booleanAssertConsumer(AbstractBooleanAssert::isTrue)
                        )
                        .hasEntrySatisfying(
                            "nodeQuery",
                            stringAssertConsumer(nodeQuery -> nodeQuery.isEqualTo(ALL_NODES_QUERY))
                        )
                        .hasEntrySatisfying("sudo", booleanAssertConsumer(AbstractBooleanAssert::isTrue))
                        .hasEntrySatisfying("logProgress", booleanAssertConsumer(AbstractBooleanAssert::isTrue))
                        .hasEntrySatisfying(
                            "readConcurrency",
                            intAssertConsumer(readConcurrency -> readConcurrency.isEqualTo(4))
                        )
                        .hasEntrySatisfying("parameters", parameters -> assertThat(parameters)
                            .asInstanceOf(InstanceOfAssertFactories.list(String.class)).isEmpty())
                        .doesNotContainKeys(
                            "username",
                            GraphProjectConfig.NODE_COUNT_KEY,
                            GraphProjectConfig.RELATIONSHIP_COUNT_KEY
                        );

                    return true;
                }, "Assert Cypher `configuration` map"),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "degreeDistribution", map(
                    "min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                ),
                "creationTime", isA(ZonedDateTime.class),
                "modificationTime", isA(ZonedDateTime.class),
                "memoryUsage", instanceOf(String.class),
                "sizeInBytes", instanceOf(Long.class),
                "density", new Condition<>(Double::isFinite, "a finite double")
            )
        ));
    }

    @Test
    void listCypherAggregation() {
        runQuery("MATCH (n0)-->(n1) WITH gds.alpha.graph.project('g', n0, n1) AS g RETURN *");

        assertCypherResult("CALL gds.graph.list()", singletonList(
            map(
                "graphName", "g",
                "database", "neo4j",
                "schema", map(
                    "nodes", map(ALL_NODES.name, map()),
                    "relationships", map(ALL_RELATIONSHIPS.name, map()),
                    "graphProperties", map()
                ),
                "schemaWithOrientation", map(
                    "nodes", map(ALL_NODES.name, map()),
                    "relationships", map(ALL_RELATIONSHIPS.name, map("properties", map(), "direction", Direction.DIRECTED.toString())),
                    "graphProperties", map()
                ),
                "configuration", new Condition<>(config -> {
                    assertThat(config)
                        .asInstanceOf(stringObjectMapAssertFactory())
                        .hasSize(5)
                        .hasEntrySatisfying("creationTime", creationTimeAssertConsumer())
                        .hasEntrySatisfying("jobId", jobId -> assertThat(jobId).isNotNull())
                        .hasEntrySatisfying("undirectedRelationshipTypes", t -> assertThat(t).isEqualTo(List.of()))
                        .hasEntrySatisfying("inverseIndexedRelationshipTypes", t -> assertThat(t).isEqualTo(List.of()))
                        .hasEntrySatisfying("logProgress", booleanAssertConsumer(AbstractBooleanAssert::isTrue));

                    return true;
                }, "Assert Cypher Aggregation `configuration` map"),
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "degreeDistribution", map(
                    "min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                ),
                "creationTime", isA(ZonedDateTime.class),
                "modificationTime", isA(ZonedDateTime.class),
                "memoryUsage", instanceOf(String.class),
                "sizeInBytes", instanceOf(Long.class),
                "density", new Condition<>(Double::isFinite, "a finite double")
            )
        ));
    }

    @Test
    void listCypherProjectionWithProperties() {
        String name = "name";
        runQuery(
            "CALL gds.graph.project.cypher($name, $nodeQuery, $relationshipQuery)",
            map(
                "name",
                name,
                "nodeQuery",
                "MATCH (n) RETURN id(n) AS id, n.foo as foo, labels(n) as labels",
                "relationshipQuery",
                "MATCH (a)-[r]->(b) RETURN id(a) AS source, id(b) AS target, r.bar as bar, type(r) as type"
            )
        );

        assertCypherResult("CALL gds.graph.list() YIELD schema", singletonList(
            map(
                "schema", map(
                    "nodes", map("A", map("foo", "Integer (DefaultValue(-9223372036854775808), TRANSIENT)")),
                    "relationships", map("REL", map("bar", "Float (DefaultValue(NaN), TRANSIENT, Aggregation.NONE)")),
                    "graphProperties", map()
                )
            )
        ));
    }

    @Test
    void listCypherProjectionProjectAllWithProperties() {
        String name = "name";
        runQuery(
            "CALL gds.graph.project.cypher($name, $nodeQuery, $relationshipQuery)",
            map(
                "name", name,
                "nodeQuery", "MATCH (n) RETURN id(n) AS id, n.foo as foo",
                "relationshipQuery", "MATCH (a)-[r]->(b) RETURN id(a) AS source, id(b) AS target, r.bar as bar"
            )
        );

        assertCypherResult("CALL gds.graph.list() YIELD schema", singletonList(
            map(
                "schema", map(
                    "nodes", map(ALL_NODES.name, map("foo", "Integer (DefaultValue(-9223372036854775808), TRANSIENT)")),
                    "relationships", map(ALL_RELATIONSHIPS.name, map("bar", "Float (DefaultValue(NaN), TRANSIENT, Aggregation.NONE)")),
                    "graphProperties", map()
                )
            )
        ));
    }

    @Test
    void degreeDistributionComputationIsOptOut() {
        String name = "name";
        runQuery("CALL gds.graph.project($name, 'A', 'REL')", map("name", name));

        assertCypherResult(
            "CALL gds.graph.list() YIELD graphName, nodeCount, relationshipCount",
            singletonList(
                map(
                    "graphName", name,
                    "nodeCount", 2L,
                    "relationshipCount", 1L
                )
            )
        );
    }

    @Test
    void calculateDegreeDistributionForUndirectedNodesWhenAskedTo() {
        String name = "name";
        runQuery("CALL gds.graph.project($name, 'A', {REL: {orientation: 'undirected'}})", map("name", name));

        assertCypherResult("CALL gds.graph.list() YIELD degreeDistribution", singletonList(
            map(
                "degreeDistribution", map(
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
            )
        ));
    }

    @Test
    void calculateDegreeDistributionForOutgoingRelationshipsWhenAskedTo() {
        String name = "name";
        runQuery("CALL gds.graph.project($name, 'A', {REL: {orientation: 'natural'}})", map("name", name));

        assertCypherResult("CALL gds.graph.list() YIELD degreeDistribution", singletonList(
            map(
                "degreeDistribution", map(
                    "min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                )
            )
        ));
    }

    @Test
    void calculateDegreeDistributionForIncomingRelationshipsWhenAskedTo() {
        String name = "name";
        runQuery("CALL gds.graph.project($name, 'A', {REL: {orientation: 'reverse'}})", map("name", name));

        assertCypherResult("CALL gds.graph.list() YIELD degreeDistribution", singletonList(
            map(
                "degreeDistribution", map(
                    "min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                )
            )
        ));
    }

    @Test
    void calculateActualMemoryUsage() {
        runQuery("CALL gds.graph.project('name', 'A', 'REL')");
        assertCypherResult(
            "CALL gds.graph.list() YIELD memoryUsage, sizeInBytes",
            List.of(Map.of(
                "memoryUsage", instanceOf(String.class),
                "sizeInBytes", instanceOf(Long.class)
            ))
        );
    }

    @ParameterizedTest(name = "name argument: {0}")
    @ValueSource(strings = {"", "null"})
    void listAllGraphsWhenCalledWithoutArgumentOrAnEmptyArgument(String argument) {
        String[] names = {"a", "b", "c"};
        for (String name : names) {
            runQuery("CALL gds.graph.project($name, 'A', 'REL')", map("name", name));
        }

        List<String> actualNames = runQuery(
            db,
            "CALL gds.graph.list(" + argument + ")",
            Collections.emptyMap(),
            result -> result.<String>columnAs("graphName")
                .stream()
                .collect(toList())
        );

        assertThat(actualNames).containsExactlyInAnyOrder(names);
    }

    @Test
    void filterOnExactMatchUsingTheFirstArgument() {
        String[] names = {"b", "bb", "ab", "ba", "B", "ʙ"};
        for (String name : names) {
            runQuery("CALL gds.graph.project($name, 'A', 'REL')", map("name", name));
        }

        String name = names[0];
        List<String> actualNames = runQuery(
            db,
            "CALL gds.graph.list($name)",
            map("name", name),
            result -> result.<String>columnAs("graphName")
                .stream()
                .collect(toList())
        );

        assertThat(actualNames).hasSize(1);
        assertThat(actualNames).contains(name);
    }


    @ParameterizedTest(name = "name argument: ''{0}''")
    @ValueSource(strings = {"foobar", "aa"})
    void returnEmptyStreamWhenNoGraphMatchesTheFilterArgument(String argument) {
        String[] names = {"a", "b", "c"};
        for (String name : names) {
            runQuery("CALL gds.graph.project($name, 'A', 'REL')", map("name", name));
        }

        long numberOfRows = runQuery(
            db,
            "CALL gds.graph.list($argument)",
            map("argument", argument),
            result -> result.stream().count()
        );

        assertThat(numberOfRows).isEqualTo(0L);
    }


    @Test
    void shouldShowSchemaForNativeProjectedGraph() {
        String loadQuery = "CALL gds.graph.project('graph', '*', '*')";

        runQuery(loadQuery);

        assertCypherResult("CALL gds.graph.list() YIELD schema",
            Collections.singletonList(map(
                "schema", map(
                    "nodes", map(ALL_NODES.name, map()),
                    "relationships", map(ALL_RELATIONSHIPS.name, map()),
                    "graphProperties", map()
            )))
        );
    }

    @Test
    void shouldShowSchemaForMultipleProjectionsWithStar() {
        runQuery("CREATE (:B {age: 12})-[:LIKES {since: 42}]->(:B {age: 66})");

        String loadQuery = "CALL gds.graph.project(" +
                           "    'graph', " +
                           "    {all: {label: '*', properties: 'foo'}, B: {properties: 'age'}}, " +
                           "    {all: {type:  '*', properties: 'since'}, REL: {properties: 'bar'}})";

        runQuery(loadQuery);

        assertCypherResult(
            "CALL gds.graph.list() YIELD schema",
            Collections.singletonList(map(
                "schema", map(
                    "nodes", map(
                        "all", map("foo", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)"),
                        "B", map("age", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)")
                    ),
                    "relationships", map(
                        "all", map("since", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)"),
                        "REL", map("bar", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)")
                    ),
                    "graphProperties", map()
                ))
            )
        );
    }

    @Test
    void shouldShowSchemaForMultipleProjectionsWithTwoRenamedStars() {
        runQuery("CREATE (:B {age: 12})-[:LIKES {since: 42}]->(:B {age: 66})");

        String loadQuery = "CALL gds.graph.project(" +
                           "    'graph', " +
                           "    {all: {label: '*', properties: 'foo'}, B: {label: '*', properties: 'age'}}, " +
                           "    {all: {type:  '*', properties: 'since'}, REL: {type: '*', properties: 'bar'}})";

        runQuery(loadQuery);

        assertCypherResult("CALL gds.graph.list() YIELD schema",
            Collections.singletonList(map(
                "schema", map(
                    "nodes", map("all", map("foo", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)"), "B", map("age", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)")),
                    "relationships",  map(
                        "all", map("since", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)"),
                        "REL", map("bar", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)")),
                    "graphProperties", map()
                )))
        );
    }

    @Test
    void shouldShowSchemaForMultipleProjections() {
        runQuery("CREATE (:B {age: 12})-[:LIKES {since: 42}]->(:B {age: 66})");

        String loadQuery = "CALL gds.graph.project(" +
                           "    'graph', " +
                           "    {A: {properties: 'foo'}, B: {properties: 'age'}}, " +
                           "    {LIKES: {properties: 'since'}, REL: {properties: 'bar'}})";

        runQuery(loadQuery);

        assertCypherResult("CALL gds.graph.list() YIELD schema",
            Collections.singletonList(map(
                "schema", map(
                    "nodes", map("A", map("foo", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)"), "B", map("age", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)")),
                    "relationships",  map(
                        "LIKES", map("since", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)"),
                        "REL", map("bar", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)")
                    ),
                    "graphProperties", map()
                )))
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.catalog.GraphProjectProcTest#invalidGraphNames")
    void failsOnInvalidGraphName(String invalidName) {
        if (invalidName != null) { // null is not a valid name, but we use it to mean 'list all'
            assertError(
                "CALL gds.graph.list($graphName)",
                map("graphName", invalidName),
                formatWithLocale("`graphName` can not be null or blank, but it was `%s`", invalidName)
            );
        }
    }

    @ParameterizedTest(name = "Invalid Graph Name: {0}")
    @ValueSource(strings = {"{ a: 'b' }", "[]", "1", "true", "false", "[1, 2, 3]", "1.4"})
    void failsOnInvalidGraphNameTypeDueToObjectSignature(String graphName) {
        assertError(formatWithLocale("CALL gds.graph.list(%s)", graphName), "Type mismatch: expected String but was");
    }

}
