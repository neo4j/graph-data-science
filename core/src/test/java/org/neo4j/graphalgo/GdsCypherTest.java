/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.proc.MapConverter;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class GdsCypherTest {

    private static final GraphCreateConfig EMPTY_GRAPH_CREATE =
        GraphCreateConfig.emptyWithName("", "");

    static Stream<Arguments> testExplicitCreationWithAnyName() {
        //@formatter:off
        return Stream.of(
            arguments("graphName" , "\"graphName\""),
            arguments("foo.bar"   , "\"foo.bar\""),
            arguments("  spa ces ", "\"  spa ces \""),
            arguments("space's"   , "\"space's\""),
            arguments("space\"s"  , "'space\"s'"),
            arguments(""          , "\"\""),
            arguments("''"        , "\"''\""),
            arguments("\"\""      , "'\"\"'"),
            arguments("🙈"        , "\"🙈\"")
        );
        //@formatter:on
    }

    @ParameterizedTest
    @MethodSource("testExplicitCreationWithAnyName")
    void testExplicitCreationWithAnyName(String graphName, String expectedStringLiteral) {
        String query = GdsCypher
            .call()
            .explicitCreation(graphName)
            .algo("algoName")
            .writeMode()
            .yields();

        assertEquals(
            String.format("CALL gds.algo.algoName.write(%s)", expectedStringLiteral),
            query
        );
    }

    static Stream<Arguments> implicitBuilders() {
        String configString =
            "{" +
            "  nodeProjection: {" +
            "    FooNode: {" +
            "      label: 'Foo'," +
            "      properties: {" +
            "        nodeProp: {" +
            "          property: 'NodePropertyName'," +
            "          defaultValue: 42.1337" +
            "        }" +
            "      }" +
            "    }" +
            "  }," +
            "  relationshipProjection: {" +
            "    Rel: 'TYPE'," +
            "    BarRel: {" +
            "      type: 'Bar'," +
            "      projection: 'UNDIRECTED'," +
            "      aggregation: 'SINGLE'," +
            "      properties: {" +
            "        relProp: {" +
            "          property: 'RelationshipPropertyName'," +
            "          defaultValue: 1337.0," +
            "          aggregation: 'MAX'" +
            "        }" +
            "      }" +
            "    }" +
            "  }," +
            "  nodeProperties: ['GlobalNodeProp']," +
            "  relationshipProperties: {" +
            "    global: 'RelProp'" +
            "  }" +
            "}";

        @SuppressWarnings("unchecked") Map<String, Object> map =
            (Map<String, Object>) new MapConverter().apply(configString).value();
        GraphCreateConfig parsedConfig = ImmutableGraphCreateConfig
            .builder()
            .username("")
            .graphName("")
            .nodeProjection(NodeProjections.fromObject(map.get("nodeProjection")))
            .relationshipProjection(RelationshipProjections.fromObject(map.get("relationshipProjection")))
            .nodeProperties(PropertyMappings.fromObject(map.get("nodeProperties")))
            .relationshipProperties(PropertyMappings.fromObject(map.get("relationshipProperties")))
            .build();

        NodeProjection fooNode = NodeProjection.builder()
            .label("Foo")
            .addProperty("nodeProp", "NodePropertyName", 42.1337)
            .build();

        RelationshipProjection barRel = RelationshipProjection.builder()
            .type("Bar")
            .projection(Projection.UNDIRECTED)
            .aggregation(DeduplicationStrategy.SINGLE)
            .addProperty("relProp", "RelationshipPropertyName", 1337, DeduplicationStrategy.MAX)
            .build();

        GraphCreateConfig configFromBuilder = ImmutableGraphCreateConfig
            .builder()
            .username("")
            .graphName("")
            .nodeProjection(NodeProjections.create(Collections.singletonMap(
                new ElementIdentifier("FooNode"), fooNode
            )))
            .nodeProperties(PropertyMappings.of(ImmutablePropertyMapping
                .builder()
                .propertyKey("GlobalNodeProp")
                .build()
            ))
            .relationshipProjection(RelationshipProjections
                .builder()
                .putProjection(
                    new ElementIdentifier("Rel"),
                    RelationshipProjection.builder().type("TYPE").build()
                )
                .putProjection(
                    new ElementIdentifier("BarRel"),
                    barRel
                )
                .build()
            )
            .relationshipProperties(PropertyMappings.of(ImmutablePropertyMapping
                .builder()
                .propertyKey("global")
                .neoPropertyKey("RelProp")
                .build()
            ))
            .build();


        return Stream.of(
            arguments(
                GdsCypher.call().implicitCreation(parsedConfig),
                "implicit config parsed cypher string"
            ),
            arguments(
                GdsCypher.call().implicitCreation(configFromBuilder),
                "implicit config created from builder"
            ),
            arguments(
                GdsCypher
                    .call()
                    .withNodeLabel("FooNode", fooNode)
                    .withNodeProperty("GlobalNodeProp")
                    .withRelationshipType("Rel", "TYPE")
                    .withRelationshipType("BarRel", barRel)
                    .withRelationshipProperty("global", "RelProp"),
                "implicit config from inlined builder in GdsCypher"
            )
        );
    }


    @ParameterizedTest(name = "{1}")
    @MethodSource("implicitBuilders")
    void testImplicitGraphCreationWithConfig(GdsCypher.QueryBuilder queryBuilder, String testName) {
        String query = queryBuilder
            .algo("algoName")
            .writeMode()
            .yields();

        assertEquals(
            String.format("CALL gds.algo.algoName.write(%s)", expectedImplicitGraphCreateCall()),
            query
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("implicitBuilders")
    void generatesGraphCreateFromImplicitConfig(GdsCypher.QueryBuilder queryBuilder, String testName) {
        String query = queryBuilder
            .graphCreate("foo42")
            .addParameter("nodeProjection", "SOMETHING | ELSE")
            .yields();

        assertEquals(
            String.format(
                "CALL gds.graph.create(\"foo42\", %s, %s, {nodeProjection: \"SOMETHING | ELSE\"})",
                expectedNodeProjection(),
                expectedRelationshipProjection()
            ),
            query
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("implicitBuilders")
    void generatesBetaGraphCreateFromImplicitConfig(GdsCypher.QueryBuilder queryBuilder, String testName) {
        String query = queryBuilder
            .betaGraphCreate("foo42")
            .addParameter("nodeProjection", "SOMETHING | ELSE")
            .yields();

        assertEquals(
            String.format(
                "CALL algo.beta.graph.create(\"foo42\", %s, %s, {nodeProjection: \"SOMETHING | ELSE\"})",
                expectedNodeProjection(),
                expectedRelationshipProjection()
            ),
            query
        );
    }

    private String expectedImplicitGraphCreateCall() {
        //@formatter:off
        return
            "{" +
                "nodeProjection: " +
                    expectedNodeProjection() +
                ", " +
                "relationshipProjection: " +
                    expectedRelationshipProjection() +
            "}";
            //@formatter:on
    }

    private String expectedNodeProjection() {
        //@formatter:off
        return
            "{" +
                "FooNode: {" +
                    "label: \"Foo\", " +
                    "properties: {" +
                        "GlobalNodeProp: {" +
                            "property: \"GlobalNodeProp\", " +
                            "defaultValue: 0.0 / 0.0" +
                        "}, " +
                        "nodeProp: {" +
                            "property: \"NodePropertyName\", " +
                            "defaultValue: 42.1337" +
                        "}" +
                    "}" +
                "}" +
            "}";
            //@formatter:on
    }

    private String expectedRelationshipProjection() {
        //@formatter:off
        return
            "{" +
                "Rel: {" +
                    "type: \"TYPE\", " +
                    "projection: \"NATURAL\", " +
                    "aggregation: \"DEFAULT\", " +
                    "properties: {" +
                        "global: {" +
                            "property: \"RelProp\", " +
                            "defaultValue: 0.0 / 0.0, " +
                            "aggregation: \"DEFAULT\"" +
                        "}" +
                    "}" +
                "}, " +
                "BarRel: {" +
                    "type: \"Bar\", " +
                    "projection: \"UNDIRECTED\", " +
                    "aggregation: \"SINGLE\", " +
                    "properties: {" +
                        "global: {" +
                            "property: \"RelProp\", " +
                            "defaultValue: 0.0 / 0.0, " +
                            "aggregation: \"DEFAULT\"" +
                        "}, " +
                        "relProp: {" +
                            "property: \"RelationshipPropertyName\", " +
                            "defaultValue: 1337.0, " +
                            "aggregation: \"MAX\"" +
                        "}" +
                    "}" +
                "}" +
            "}";
            //@formatter:on
    }

    @ParameterizedTest
    @ValueSource(strings = {"louvain", "pageRank", "", " spa ces  ", "🙈"})
    void algoNameIsInsertedVerbatim(String algoName) {
        String query = GdsCypher
            .call()
            .implicitCreation(EMPTY_GRAPH_CREATE)
            .algo(algoName)
            .writeMode()
            .yields();

        assertEquals(
            String.format("CALL gds.algo.%s.write({})", algoName),
            query
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"gds.graph.create", "algo.louvain", "geedeeess.algo.louvain", "  foo .  bar  ", "🙈.🙉.🙊"})
    void algoNameWithPeriodsOverridesDefaultNamespace(String algoName) {
        String query = GdsCypher
            .call()
            .implicitCreation(EMPTY_GRAPH_CREATE)
            .algo(algoName)
            .writeMode()
            .yields();

        assertEquals(
            String.format("CALL %s.write({})", algoName),
            query
        );
    }

    static Stream<Arguments> separateNamePartsArguments() {
        return Stream.of(
            "gds.graph.create",
            "algo.louvain",
            "geedeeess.algo.louvain",
            "  foo .  bar  ",
            "🙈.🙉.🙊"
        ).map(s -> arguments((Object) s.split(Pattern.quote("."))));
    }

    @ParameterizedTest
    @MethodSource("separateNamePartsArguments")
    void algoNamePartsCanBeSpecifiedAsSeparateArguments(String[] algoNameParts) {
        String query = GdsCypher
            .call()
            .implicitCreation(EMPTY_GRAPH_CREATE)
            .algo(algoNameParts)
            .writeMode()
            .yields();

        assertEquals(
            String.format("CALL %s.write({})", String.join(".", algoNameParts)),
            query
        );
    }

    @ParameterizedTest
    @EnumSource(GdsCypher.ExecutionModes.class)
    void testExecutionModesViaEnum(GdsCypher.ExecutionModes executionMode) {
        String query = GdsCypher
            .call()
            .implicitCreation(EMPTY_GRAPH_CREATE)
            .algo("algoName")
            .executionMode(executionMode)
            .yields();

        assertEquals(
            String.format("CALL gds.algo.algoName.%s({})", executionModeName(executionMode)),
            query
        );
    }

    @ParameterizedTest
    @EnumSource(GdsCypher.ExecutionModes.class)
    void testExecutionModesViaExplicitMethodCalls(GdsCypher.ExecutionModes executionMode) {
        GdsCypher.ModeBuildStage builder = GdsCypher.call().implicitCreation(EMPTY_GRAPH_CREATE).algo("algoName");
        GdsCypher.ParametersBuildStage nextBuilder;

        switch (executionMode) {
            case WRITE:
                nextBuilder = builder.writeMode();
                break;
            case STATS:
                nextBuilder = builder.statsMode();
                break;
            case STREAM:
                nextBuilder = builder.streamMode();
                break;
            default:
                throw new IllegalArgumentException("Unexpected value: " + executionMode + " (sad java 😞)");
        }
        String query = nextBuilder.yields();

        assertEquals(
            String.format("CALL gds.algo.algoName.%s({})", executionModeName(executionMode)),
            query
        );
    }

    @ParameterizedTest
    @EnumSource(GdsCypher.ExecutionModes.class)
    void testEstimateModesViaEnum(GdsCypher.ExecutionModes executionMode) {
        String query = GdsCypher
            .call()
            .implicitCreation(EMPTY_GRAPH_CREATE)
            .algo("algoName")
            .estimationMode(executionMode)
            .yields();

        assertEquals(
            String.format("CALL gds.algo.algoName.%s.estimate({})", executionModeName(executionMode)),
            query
        );
    }

    @ParameterizedTest
    @EnumSource(GdsCypher.ExecutionModes.class)
    void testEstimatesModesViaExplicitMethodCalls(GdsCypher.ExecutionModes executionMode) {
        GdsCypher.ModeBuildStage builder = GdsCypher.call().implicitCreation(EMPTY_GRAPH_CREATE).algo("algoName");
        GdsCypher.ParametersBuildStage nextBuilder;

        switch (executionMode) {
            case WRITE:
                nextBuilder = builder.writeEstimation();
                break;
            case STATS:
                nextBuilder = builder.statsEstimation();
                break;
            case STREAM:
                nextBuilder = builder.streamEstimation();
                break;
            default:
                throw new IllegalArgumentException("Unexpected value: " + executionMode + " (sad java 😞)");
        }
        String query = nextBuilder.yields();

        assertEquals(
            String.format("CALL gds.algo.algoName.%s.estimate({})", executionModeName(executionMode)),
            query
        );
    }

    static Stream<Arguments> testAdditionalProperties() {
        return Stream.of(
            arguments(true, "true"),
            arguments(false, "false"),
            arguments(42, "42"),
            arguments(42.0, "42.0"),
            arguments(1337.42, "1337.42"),
            arguments(Double.NaN, "0.0 / 0.0"),
            arguments("42", "\"42\""),
            arguments(new StringBuilder("forty-two"), "\"forty-two\""),
            arguments("string with '", "\"string with '\""),
            arguments("string with \"", "'string with \"'"),
            arguments("string with both ' and \"", "\"string with both ' and \\\"\""),
            arguments(Direction.BOTH, "\"BOTH\""),
            arguments(Projection.NATURAL, "\"NATURAL\""),
            arguments(Arrays.asList("foo", 42, true), "[\"foo\", 42, true]"),
            arguments(MapUtil.map(new LinkedHashMap<>(), "foo", 42, "bar", true), "{foo: 42, bar: true}")
        );
    }

    @ParameterizedTest
    @MethodSource("testAdditionalProperties")
    void testAdditionalProperties(Object value, String expected) {
        String query = GdsCypher
            .call()
            .explicitCreation("")
            .algo("algoName")
            .writeMode()
            .addParameter("foo", value)
            .addParameter(new AbstractMap.SimpleImmutableEntry<>("bar", value))
            .addAllParameters(Collections.singletonMap("baz", value))
            .yields();

        assertEquals(
            String.format("CALL gds.algo.algoName.write(\"\", {foo: %1$s, bar: %1$s, baz: %1$s})", expected),
            query
        );
    }

    static Stream<Object> testEmptyProperties() {
        return Stream.of(
            Collections.emptyList(),
            Collections.emptySet(),
            Collections.emptyMap()
        );
    }

    @ParameterizedTest
    @MethodSource("testEmptyProperties")
    void testEmptyProperties(Object value) {
        String query = GdsCypher
            .call()
            .explicitCreation("")
            .algo("algoName")
            .writeMode()
            .addParameter("foo", value)
            .addParameter(new AbstractMap.SimpleImmutableEntry<>("bar", value))
            .addAllParameters(Collections.singletonMap("baz", value))
            .yields();

        assertEquals(
            "CALL gds.algo.algoName.write(\"\", {})",
            query
        );
    }

    @Test
    void testNoYield() {
        String query = GdsCypher
            .call()
            .explicitCreation("")
            .algo("algoName")
            .writeMode()
            .yields();

        assertEquals(
            "CALL gds.algo.algoName.write(\"\")",
            query
        );
    }

    static Stream<List<String>> testYields() {
        return Stream.of(
            Collections.singletonList("foo"),
            Arrays.asList("foo", "BAR"),
            Arrays.asList("foo BAR", "foo.BAR"),
            Arrays.asList("", "'", "__", "ಠ__ಠ", "🙈")
        );
    }

    @ParameterizedTest
    @MethodSource("testYields")
    void testYields(Iterable<String> yieldedFields) {
        String query = GdsCypher
            .call()
            .explicitCreation("")
            .algo("algoName")
            .writeMode()
            .yields(yieldedFields);

        assertEquals(
            String.format(
                "CALL gds.algo.algoName.write(\"\") YIELD %s",
                String.join(", ", yieldedFields)
            ),
            query
        );
    }

    private static String executionModeName(GdsCypher.ExecutionModes executionMode) {
        switch (executionMode) {
            case WRITE:
                return "write";
            case STATS:
                return "stats";
            case STREAM:
                return "stream";
            default:
                throw new IllegalArgumentException("Unexpected value: " + executionMode + " (sad java 😞)");
        }
    }
}
