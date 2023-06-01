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
import static java.util.Map.entry;
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
    void shouldCacheDegreeDistribution() {
        var name = "name";
        var generateQuery = "CALL gds.beta.graph.generate($name, 500000, 5)";
        runQuery(
            generateQuery,
            Map.of("name", name)
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
            Map.of("name", name)
        );

        var condition = new Condition<>(config -> {
            assertThat(config)
                .asInstanceOf(stringObjectMapAssertFactory())
                .hasSize(12)
                .containsEntry("nodeProjections", Map.of(
                    "10_Nodes", Map.of(
                        "label", "10_Nodes",
                        "properties", emptyMap()
                    )
                ))
                .containsEntry("relationshipProjections", Map.of(
                    "REL", Map.of(
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
        }, "Assert generated `configuration` map");

        assertCypherResult("CALL gds.graph.list()", singletonList(
            Map.ofEntries(
                entry("graphName", name),
                entry("database", "neo4j"),
                entry("configuration", condition),
                entry("schema", Map.of(
                    "nodes", Map.of("__ALL__", Map.of()),
                    "relationships", Map.of("REL", Map.of()),
                    "graphProperties", Map.of()
                )),
                entry("schemaWithOrientation", Map.of(
                    "nodes", Map.of("__ALL__", Map.of()),
                    "relationships", Map.of("REL", Map.of("direction", "DIRECTED", "properties", Map.of())),
                    "graphProperties", Map.of()
                )),
                entry("nodeCount", 10L),
                entry("relationshipCount", 50L),
                entry("degreeDistribution", Map.of(
                    "min", 5L,
                    "mean", 5.0D,
                    "max", 5L,
                    "p50", 5L,
                    "p75", 5L,
                    "p90", 5L,
                    "p95", 5L,
                    "p99", 5L,
                    "p999", 5L
                )),
                entry("creationTime", isA(ZonedDateTime.class)),
                entry("modificationTime", isA(ZonedDateTime.class)),
                entry("memoryUsage", instanceOf(String.class)),
                entry("sizeInBytes", instanceOf(Long.class)),
                entry("density", new Condition<>(Double::isFinite, "a finite double"))
            )
        ));
    }

    @Test
    void listASingleLabelRelationshipTypeProjectionWithProperties() {
        String name = "name";
        runQuery(
            "CALL gds.graph.project($name, 'A', 'REL', {nodeProperties: 'foo', relationshipProperties: 'bar'})",
            Map.of("name", name)
        );

        assertCypherResult("CALL gds.graph.list() YIELD schema", singletonList(
            Map.of(
                "schema", Map.of(
                    "nodes", Map.of("A", Map.of("foo", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)")),
                    "relationships", Map.of("REL", Map.of("bar", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)")),
                    "graphProperties", Map.of()
                )
            )
        ));
    }

    @Test
    void listCypherProjection() {
        String name = "name";
        runQuery(
            "CALL gds.graph.project.cypher($name, $nodeQuery, $relationshipQuery)",
            Map.of("name", name, "nodeQuery", ALL_NODES_QUERY, "relationshipQuery", ALL_RELATIONSHIPS_QUERY)
        );

        var condition = new Condition<>(config -> {
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
        }, "Assert Cypher `configuration` map");

        assertCypherResult("CALL gds.graph.list()", singletonList(
            Map.ofEntries(
                entry("graphName", name),
                entry("database", "neo4j"),
                entry("schema", Map.of(
                    "nodes", Map.of(ALL_NODES.name, Map.of()),
                    "relationships", Map.of(ALL_RELATIONSHIPS.name, Map.of()),
                    "graphProperties", Map.of()
                )),
                entry("schemaWithOrientation", Map.of(
                    "nodes", Map.of(ALL_NODES.name, Map.of()),
                    "relationships", Map.of(ALL_RELATIONSHIPS.name, Map.of("direction", "DIRECTED", "properties", Map.of())),
                    "graphProperties", Map.of()
                )),
                entry("configuration", condition),
                entry("nodeCount", 2L),
                entry("relationshipCount", 1L),
                entry("degreeDistribution", Map.of(
                    "min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                )),
                entry("creationTime", isA(ZonedDateTime.class)),
                entry("modificationTime", isA(ZonedDateTime.class)),
                entry("memoryUsage", instanceOf(String.class)),
                entry("sizeInBytes", instanceOf(Long.class)),
                entry("density", new Condition<>(Double::isFinite, "a finite double"))
            )
        ));
    }

    @Test
    void listCypherAggregation() {
        runQuery("MATCH (n0)-->(n1) WITH gds.graph.project('g', n0, n1) AS g RETURN *");

        var condition = new Condition<>(config -> {
            assertThat(config)
                .asInstanceOf(stringObjectMapAssertFactory())
                .hasSize(5)
                .hasEntrySatisfying("creationTime", creationTimeAssertConsumer())
                .hasEntrySatisfying("jobId", jobId -> assertThat(jobId).isNotNull())
                .hasEntrySatisfying("undirectedRelationshipTypes", t -> assertThat(t).isEqualTo(List.of()))
                .hasEntrySatisfying("inverseIndexedRelationshipTypes", t -> assertThat(t).isEqualTo(List.of()))
                .hasEntrySatisfying("logProgress", booleanAssertConsumer(AbstractBooleanAssert::isTrue));

            return true;
        }, "Assert Cypher Aggregation `configuration` map");

        assertCypherResult("CALL gds.graph.list()", singletonList(
            Map.ofEntries(
                entry("graphName", "g"),
                entry("database", "neo4j"),
                entry("schema", Map.of(
                    "nodes", Map.of(ALL_NODES.name, Map.of()),
                    "relationships", Map.of(ALL_RELATIONSHIPS.name, Map.of()),
                    "graphProperties", Map.of()
                )),
                entry("schemaWithOrientation", Map.of(
                    "nodes", Map.of(ALL_NODES.name, Map.of()),
                    "relationships", Map.of(ALL_RELATIONSHIPS.name, Map.of("properties", Map.of(), "direction", Direction.DIRECTED.toString())),
                    "graphProperties", Map.of()
                )),
                entry("configuration", condition),
                entry("nodeCount", 2L),
                entry("relationshipCount", 1L),
                entry("degreeDistribution", Map.of(
                    "min", 0L,
                    "mean", 0.5D,
                    "max", 1L,
                    "p50", 0L,
                    "p75", 1L,
                    "p90", 1L,
                    "p95", 1L,
                    "p99", 1L,
                    "p999", 1L
                )),
                entry("creationTime", isA(ZonedDateTime.class)),
                entry("modificationTime", isA(ZonedDateTime.class)),
                entry("memoryUsage", instanceOf(String.class)),
                entry("sizeInBytes", instanceOf(Long.class)),
                entry("density", new Condition<>(Double::isFinite, "a finite double"))
            )
        ));
    }

    @Test
    void listCypherProjectionWithProperties() {
        String name = "name";
        runQuery(
            "CALL gds.graph.project.cypher($name, $nodeQuery, $relationshipQuery)",
            Map.of(
                "name",
                name,
                "nodeQuery",
                "MATCH (n) RETURN id(n) AS id, n.foo as foo, labels(n) as labels",
                "relationshipQuery",
                "MATCH (a)-[r]->(b) RETURN id(a) AS source, id(b) AS target, r.bar as bar, type(r) as type"
            )
        );

        assertCypherResult("CALL gds.graph.list() YIELD schema", singletonList(
            Map.of(
                "schema", Map.of(
                    "nodes", Map.of("A", Map.of("foo", "Integer (DefaultValue(-9223372036854775808), TRANSIENT)")),
                    "relationships", Map.of("REL", Map.of("bar", "Float (DefaultValue(NaN), TRANSIENT, Aggregation.NONE)")),
                    "graphProperties", Map.of()
                )
            )
        ));
    }

    @Test
    void listCypherProjectionProjectAllWithProperties() {
        String name = "name";
        runQuery(
            "CALL gds.graph.project.cypher($name, $nodeQuery, $relationshipQuery)",
            Map.of(
                "name", name,
                "nodeQuery", "MATCH (n) RETURN id(n) AS id, n.foo as foo",
                "relationshipQuery", "MATCH (a)-[r]->(b) RETURN id(a) AS source, id(b) AS target, r.bar as bar"
            )
        );

        assertCypherResult("CALL gds.graph.list() YIELD schema", singletonList(
            Map.of(
                "schema", Map.of(
                    "nodes", Map.of(ALL_NODES.name, Map.of("foo", "Integer (DefaultValue(-9223372036854775808), TRANSIENT)")),
                    "relationships", Map.of(ALL_RELATIONSHIPS.name, Map.of("bar", "Float (DefaultValue(NaN), TRANSIENT, Aggregation.NONE)")),
                    "graphProperties", Map.of()
                )
            )
        ));
    }


    @Test
    void shouldShowSchemaForNativeProjectedGraph() {
        String loadQuery = "CALL gds.graph.project('graph', '*', '*')";

        runQuery(loadQuery);

        assertCypherResult("CALL gds.graph.list() YIELD schema",
            Collections.singletonList(Map.of(
                "schema", Map.of(
                    "nodes", Map.of(ALL_NODES.name, Map.of()),
                    "relationships", Map.of(ALL_RELATIONSHIPS.name, Map.of()),
                    "graphProperties", Map.of()
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
            Collections.singletonList(Map.of(
                "schema", Map.of(
                    "nodes", Map.of(
                        "all", Map.of("foo", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)"),
                        "B", Map.of("age", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)")
                    ),
                    "relationships", Map.of(
                        "all", Map.of("since", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)"),
                        "REL", Map.of("bar", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)")
                    ),
                    "graphProperties", Map.of()
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
            Collections.singletonList(Map.of(
                "schema", Map.of(
                    "nodes", Map.of("all", Map.of("foo", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)"), "B", Map.of("age", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)")),
                    "relationships",  Map.of(
                        "all", Map.of("since", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)"),
                        "REL", Map.of("bar", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)")),
                    "graphProperties", Map.of()
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
            Collections.singletonList(Map.of(
                "schema", Map.of(
                    "nodes", Map.of("A", Map.of("foo", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)"), "B", Map.of("age", "Integer (DefaultValue(-9223372036854775808), PERSISTENT)")),
                    "relationships",  Map.of(
                        "LIKES", Map.of("since", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)"),
                        "REL", Map.of("bar", "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)")
                    ),
                    "graphProperties", Map.of()
                )))
        );
    }


    @ParameterizedTest(name = "Invalid Graph Name: {0}")
    @ValueSource(strings = {"{ a: 'b' }", "[]", "1", "true", "false", "[1, 2, 3]", "1.4"})
    void failsOnInvalidGraphNameTypeDueToObjectSignature(String graphName) {
        assertError(formatWithLocale("CALL gds.graph.list(%s)", graphName), "Type mismatch: expected String but was");
    }

}
