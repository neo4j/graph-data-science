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
package org.neo4j.gds;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.graphdb.Direction;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class GdsCypherTest {

    @SuppressWarnings("checkstyle:NoWhitespaceBefore")
    static Stream<Arguments> testGraphNames() {
        //@formatter:off
        return Stream.of(
            arguments("graphName" , "'graphName'"),
            arguments("foo.bar"   , "'foo.bar'"),
            arguments("  spa ces ", "'  spa ces '"),
            arguments("space's"   , "'space\\'s'"),
            arguments("space\"s"  , "'space\\\"s'"),
            arguments(""          , "''"),
            arguments("''"        , "'\\'\\''"),
            arguments("\"\""      , "'\\\"\\\"'"),
            arguments("🙈"        , "'🙈'")
        );
        //@formatter:on
    }

    @ParameterizedTest
    @MethodSource("testGraphNames")
    void testAlgoCallWithAnyGraphName(String graphName, String expectedStringLiteral) {
        String query = GdsCypher
            .call(graphName)
            .algo("algoName")
            .writeMode()
            .yields();

        assertThat(query).isEqualTo("CALL gds.algoName.write(%s)", expectedStringLiteral);
    }

    @Test
    void loadEverythingShortcut() {
        String query = GdsCypher
            .call("graph")
            .algo("foo")
            .writeMode()
            .yields();

        assertThat(query).isEqualTo("CALL gds.foo.write('graph')");
    }

    @ParameterizedTest
    @ValueSource(strings = {"louvain", "pageRank", "", " spa ces  ", "🙈"})
    void algoNameIsInsertedVerbatim(String algoName) {
        String query = GdsCypher
            .call("graph")
            .algo(algoName)
            .writeMode()
            .yields();

        assertThat(query).isEqualTo("CALL gds.%s.write('graph')", algoName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"gds.graph.project", "algo.louvain", "geedeeess.algo.louvain", "  foo .  bar  ", "🙈.🙉.🙊"})
    void algoNameWithPeriodsOverridesDefaultNamespace(String algoName) {
        String query = GdsCypher
            .call("graph")
            .algo(algoName)
            .writeMode()
            .yields();

        assertThat(query).isEqualTo("CALL %s.write('graph')", algoName);
    }

    static Stream<Arguments> separateNamePartsArguments() {
        return Stream.of(
            "gds.graph.project",
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
            .call("graph")
            .algo(algoNameParts)
            .writeMode()
            .yields();

        assertThat(query)
            .isEqualTo("CALL %s.write('graph')", String.join(".", algoNameParts));
    }

    @ParameterizedTest
    @EnumSource(GdsCypher.ExecutionModes.class)
    void testExecutionModesViaEnum(GdsCypher.ExecutionModes executionMode) {
        String query = GdsCypher
            .call("graph")
            .algo("algoName")
            .executionMode(executionMode)
            .yields();

        assertThat(query).isEqualTo(
            "CALL gds.algoName.%s('graph')",
            executionModeName(executionMode)
        );
    }

    @ParameterizedTest
    @EnumSource(GdsCypher.ExecutionModes.class)
    void testExecutionModesViaExplicitMethodCalls(GdsCypher.ExecutionModes executionMode) {
        GdsCypher.ModeBuildStage builder = GdsCypher
            .call("graph")
            .algo("algoName");
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
            case MUTATE:
                nextBuilder = builder.mutateMode();
                break;
            case TRAIN:
                nextBuilder = builder.trainMode();
                break;
            default:
                throw new IllegalArgumentException("Unexpected value: " + executionMode + " (sad java 😞)");
        }
        String query = nextBuilder.yields();

        assertThat(query).isEqualTo(
            "CALL gds.algoName.%s('graph')",
            executionModeName(executionMode)
        );
    }

    @ParameterizedTest
    @EnumSource(GdsCypher.ExecutionModes.class)
    void testEstimateModesViaEnum(GdsCypher.ExecutionModes executionMode) {
        String query = GdsCypher
            .call("graph")
            .algo("algoName")
            .estimationMode(executionMode)
            .yields();

        assertThat(query).isEqualTo(
            "CALL gds.algoName.%s.estimate('graph', {})",
            executionModeName(executionMode)
        );
    }

    @ParameterizedTest
    @EnumSource(GdsCypher.ExecutionModes.class)
    void testEstimatesModesViaExplicitMethodCalls(GdsCypher.ExecutionModes executionMode) {
        GdsCypher.ModeBuildStage builder = GdsCypher
            .call("graph")
            .algo("algoName");
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
            case MUTATE:
                nextBuilder = builder.mutateEstimation();
                break;
            case TRAIN:
                nextBuilder = builder.trainEstimation();
                break;
            default:
                throw new IllegalArgumentException("Unexpected value: " + executionMode + " (sad java 😞)");
        }
        String query = nextBuilder.yields();

        assertThat(query).isEqualTo(
            "CALL gds.algoName.%s.estimate('graph', {})",
            executionModeName(executionMode)
        );
    }

    static Stream<Arguments> testAdditionalProperties() {
        return Stream.of(
            arguments(true, "true"),
            arguments(false, "false"),
            arguments(42, "42"),
            arguments(42.0, "42.0"),
            arguments(1337.42, "1337.42"),
            arguments(Double.NaN, "(0.0 / 0.0)"),
            arguments("42", "'42'"),
            arguments(new StringBuilder("forty-two"), "'forty-two'"),
            arguments("string with '", "'string with \\''"),
            arguments("string with \"", "'string with \\\"'"),
            arguments("string with both ' and \"", "'string with both \\' and \\\"'"),
            arguments(Direction.BOTH, "'BOTH'"),
            arguments(Orientation.NATURAL, "'NATURAL'"),
            arguments(Arrays.asList("foo", 42, true), "['foo', 42, true]"),
            arguments(MapUtil.map(new LinkedHashMap<>(), "foo", 42, "bar", true), "{foo: 42, bar: true}")
        );
    }

    @ParameterizedTest
    @MethodSource("testAdditionalProperties")
    void testAdditionalProperties(Object value, String expected) {
        String query = GdsCypher
            .call("graph")
            .algo("algoName")
            .writeMode()
            .addParameter("foo", value)
            .addParameter(new AbstractMap.SimpleImmutableEntry<>("bar", value))
            .addAllParameters(Collections.singletonMap("baz", value))
            .yields();

        assertThat(query).isEqualTo("CALL gds.algoName.write('graph', {foo: %1$s, bar: %1$s, baz: %1$s})", expected);
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
            .call("graph")
            .algo("algoName")
            .writeMode()
            .addParameter("foo", value)
            .addParameter(Map.entry("bar", value))
            .addAllParameters(Map.of("baz", value))
            .yields();

        assertThat(query).isEqualTo("CALL gds.algoName.write('graph', {})");
    }

    @SuppressWarnings("checkstyle:NoWhitespaceBefore")
    static Stream<Arguments> placeholders() {
        return Stream.of(
            //@formatter:off
            arguments("g"             , "$g"),
            arguments("var"           , "$var"),
            arguments("graphName"     , "$graphName"),
            arguments("\"$graphName\"", "$\"$graphName\""),
            arguments("'$graphName'"  , "$'$graphName'"),
            arguments("\"graphName\"" , "$\"graphName\""),
            arguments("'graphName'"   , "$'graphName'"),
            arguments("%"             , "$%")
            //@formatter:on
        );
    }

    @ParameterizedTest
    @MethodSource("placeholders")
    void testPlaceholders(String placeholder, String expected) {
        String query = GdsCypher
            .call("graph")
            .algo("algoName")
            .writeMode()
            .addPlaceholder("foo", placeholder)
            .yields();

        assertThat(query).isEqualTo("CALL gds.algoName.write('graph', {foo: %s})", expected);
    }

    static Stream<Arguments> variables() {
        return Stream.of(
            arguments("g", "g"),
            arguments("var", "var"),
            arguments("graphName", "graphName")
        );
    }

    @ParameterizedTest
    @MethodSource("variables")
    void testVariables(String variable, String expected) {
        String query = GdsCypher
            .call("graph")
            .algo("algoName")
            .writeMode()
            .addVariable("foo", variable)
            .yields();

        assertThat(query).isEqualTo("CALL gds.algoName.write('graph', {foo: %s})", expected);
    }

    @Test
    void testNoYield() {
        String query = GdsCypher
            .call("graph")
            .algo("algoName")
            .writeMode()
            .yields();

        assertThat(query).isEqualTo("CALL gds.algoName.write('graph')");
    }

    static Stream<Arguments> testYields() {
        return Stream.of(
            arguments(List.of("foo"), "foo"),
            arguments(List.of("foo", "BAR"), "foo, BAR"),
            arguments(List.of(" foo", "bar ", "  baz  ", "qux\t\r\n"), "foo, bar, baz, qux"),
            arguments(List.of("foo, bar", "baz"), "foo, bar, baz")
        );
    }

    @ParameterizedTest
    @MethodSource("testYields")
    void testYields(Iterable<String> yieldedFields, String expectedYield) {
        String query = GdsCypher
            .call("graph")
            .algo("algoName")
            .writeMode()
            .yields(yieldedFields);

        assertThat(query).isEqualTo(
            "CALL gds.algoName.write('graph') YIELD %s",
            expectedYield
        );
    }

    @Test
    void testYieldErrorOnIllegalName() {
        var builder = GdsCypher
            .call("graph")
            .algo("algoName")
            .writeMode();

        assertThatThrownBy(() -> builder.yields("123"))
            .hasMessage("`123` is not a valid Cypher name: Name must be a valid identifier.");

        assertThatThrownBy(() -> builder.yields("       "))
            .hasMessage("`` is not a valid Cypher name: Name must not be empty.");
    }

    private static String executionModeName(GdsCypher.ExecutionModes executionMode) {
        switch (executionMode) {
            case WRITE:
                return "write";
            case STATS:
                return "stats";
            case STREAM:
                return "stream";
            case MUTATE:
                return "mutate";
            case TRAIN:
                return "train";
            default:
                throw new IllegalArgumentException("Unexpected value: " + executionMode + " (sad java 😞)");
        }
    }

    static Stream<Arguments> graphProjectBuilders() {
        var configMap = Map.of(
            "nodeProjection", Map.of(
                "FooNode", Map.of(
                    "label", "Foo",
                    "properties", Map.of(
                        "nodeProp", Map.of(
                            "property", "NodePropertyName",
                            "defaultValue", 42.1337
                        )
                    )
                )
            ),
            "relationshipProjection", Map.of(
                "Rel", "TYPE",
                "BarRel", Map.of(
                    "type", "Bar",
                    "orientation", "UNDIRECTED",
                    "aggregation", "SINGLE",
                    "properties", Map.of(
                        "relProp", Map.of(
                            "property", "RelationshipPropertyName",
                            "defaultValue", 1337L,
                            "aggregation", "MAX"
                        )
                    )
                )
            ),
            "nodeProperties", List.of("GlobalNodeProp"),
            "relationshipProperties", Map.of(
                "global", "RelProp"
            )
        );

        GraphProjectFromStoreConfig parsedConfig = ImmutableGraphProjectFromStoreConfig
            .builder()
            .username("")
            .graphName("")
            .nodeProjections(NodeProjections.fromObject(configMap.get("nodeProjection")))
            .relationshipProjections(RelationshipProjections.fromObject(configMap.get("relationshipProjection")))
            .nodeProperties(PropertyMappings.fromObject(configMap.get("nodeProperties")))
            .relationshipProperties(PropertyMappings.fromObject(configMap.get("relationshipProperties")))
            .build();

        NodeProjection fooNode = NodeProjection.builder()
            .label("Foo")
            .addProperty("nodeProp", "NodePropertyName", DefaultValue.of(42.1337))
            .build();

        RelationshipProjection barRel = RelationshipProjection.builder()
            .type("Bar")
            .orientation(Orientation.UNDIRECTED)
            .aggregation(Aggregation.SINGLE)
            .addProperty("relProp", "RelationshipPropertyName", DefaultValue.of(1337), Aggregation.MAX)
            .build();

        GraphProjectFromStoreConfig configFromBuilder = ImmutableGraphProjectFromStoreConfig
            .builder()
            .username("")
            .graphName("")
            .nodeProjections(NodeProjections.create(Collections.singletonMap(
                new NodeLabel("FooNode"), fooNode
            )))
            .nodeProperties(PropertyMappings.of(ImmutablePropertyMapping
                .builder()
                .propertyKey("GlobalNodeProp")
                .build()
            ))
            .relationshipProjections(ImmutableRelationshipProjections
                .builder()
                .putProjection(
                    new RelationshipType("Rel"),
                    RelationshipProjection.builder().type("TYPE").build()
                )
                .putProjection(
                    new RelationshipType("BarRel"),
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
                GdsCypher
                    .call("graph")
                    .graphProject()
                    .withGraphProjectConfig(parsedConfig),
                "config from cypher string"
            ),
            arguments(
                GdsCypher
                    .call("graph")
                    .graphProject()
                    .withGraphProjectConfig(configFromBuilder),
                "config from builder"
            ),
            arguments(
                GdsCypher
                    .call("graph")
                    .graphProject()
                    .withNodeLabel("FooNode", fooNode)
                    .withNodeProperty("GlobalNodeProp")
                    .withRelationshipType("Rel", "TYPE")
                    .withRelationshipType("BarRel", barRel)
                    .withRelationshipProperty("global", "RelProp"),
                "config from inlined builder in GdsCypher"
            )
        );
    }

    private String expectedNodeProjection() {
        //@formatter:off
        return
            "{" +
              "FooNode: {" +
                "label: 'Foo', " +
                "properties: {" +
                  "nodeProp: {" +
                    "property: 'NodePropertyName', " +
                    "defaultValue: 42.1337" +
                  "}, " +
                  "GlobalNodeProp: {" +
                    "property: 'GlobalNodeProp'" +
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
                "type: 'TYPE', " +
                "properties: {" +
                  "global: {" +
                    "property: 'RelProp'" +
                  "}" +
                "}" +
              "}, " +
              "BarRel: {" +
                "type: 'Bar', " +
                "orientation: 'UNDIRECTED', " +
                "aggregation: 'SINGLE', " +
                "properties: {" +
                  "relProp: {" +
                    "property: 'RelationshipPropertyName', " +
                    "defaultValue: 1337, " +
                    "aggregation: 'MAX'" +
                  "}, " +
                  "global: {" +
                    "property: 'RelProp', " +
                    "aggregation: 'SINGLE'" +
                  "}" +
                "}" +
              "}" +
            "}";
        //@formatter:on
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("graphProjectBuilders")
    void generatesGraphProjectFromImplicitConfig(GdsCypher.GraphProjectBuilder queryBuilder, String testName) {
        String query = queryBuilder
            .addParameter("nodeProjection", "SOMETHING | ELSE")
            .yields();

        assertThat(query).isEqualTo(
            "CALL gds.graph.project('graph', %s, %s, {nodeProjection: 'SOMETHING | ELSE'})",
            expectedNodeProjection(),
            expectedRelationshipProjection()
        );
    }

    @Test
    void loadEverything() {
        String query = GdsCypher
            .call("graph")
            .graphProject()
            .yields();

        assertThat(query).isEqualTo("CALL gds.graph.project('graph', '*', '*')");
    }

    @Test
    void loadEverythingWithRelationshipProperty() {
        String query = GdsCypher
            .call("graph")
            .graphProject()
            .withRelationshipProperty("weight")
            .loadEverything()
            .yields();

        assertThat(query).isEqualTo(
            //@formatter:off
            "CALL gds.graph.project(" +
              "'graph', " +
              "'*', " +
              "{" +
                "__ALL__: {" +
                  "type: '*', " +
                  "properties: 'weight'" +
                "}" +
              "}" +
            ")"
            //@formatter:on
        );
    }

    @Test
    void testExplicitNanDefaultValueInNodeProperties() {
        String query = GdsCypher
            .call("graph")
            .graphProject()
            .withNodeLabel("N")
            .withAnyRelationshipType()
            .withNodeProperties(List.of("a", "b"), DefaultValue.of(Double.NaN))
            .yields();
        assertThat(query).isEqualTo(
            "CALL gds.graph.project('graph', {" +
            "N: {label: 'N', properties: {" +
            "a: {property: 'a', defaultValue: (0.0 / 0.0)}, " +
            "b: {property: 'b', defaultValue: (0.0 / 0.0)}" +
            "}}}, '*')"
        );

    }
}
