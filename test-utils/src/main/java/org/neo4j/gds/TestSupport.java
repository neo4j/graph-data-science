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

import org.assertj.core.api.Condition;
import org.assertj.core.api.HamcrestCondition;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.SoftAssertions;
import org.hamcrest.Matcher;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.canonization.CanonicalAdjacencyMatrix;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.extension.GdlSupportExtension;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.gdl.ImmutableGraphCreateFromGdlConfig;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.REVERSE;
import static org.neo4j.gds.QueryRunner.runQueryWithResultConsumer;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runInTransaction;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TestSupport {

    public enum FactoryType {
        NATIVE,
        NATIVE_BIT_ID_MAP,
        CYPHER
    }

    private TestSupport() {}

    @Retention(RetentionPolicy.RUNTIME)
    @ParameterizedTest
    @MethodSource("org.neo4j.gds.TestSupport#allFactoryTypes")
    public @interface AllGraphStoreFactoryTypesTest {}

    public static Stream<FactoryType> allFactoryTypes() {
        return Stream.of(FactoryType.NATIVE, FactoryType.NATIVE_BIT_ID_MAP, FactoryType.CYPHER);
    }

    public static Stream<Orientation> allDirectedProjections() {
        return Stream.of(NATURAL, REVERSE);
    }

    public static <T> Supplier<Stream<Arguments>> toArguments(Supplier<Stream<T>> fn) {
        return () -> fn.get().map(Arguments::of);
    }

    public static <T> Supplier<Stream<Arguments>> toArgumentsFlat(Supplier<Stream<List<T>>> fn) {
        return () -> fn.get().map(List::toArray).map(Arguments::of);
    }

    @SafeVarargs
    public static Stream<Arguments> crossArguments(Supplier<Stream<Arguments>> firstFn, Supplier<Stream<Arguments>>... otherFns) {
        return Arrays
                .stream(otherFns)
                .reduce(firstFn, (l, r) -> () -> crossArguments(l, r))
                .get();
    }

    public static Stream<Arguments> crossArguments(Supplier<Stream<Arguments>> leftFn, Supplier<Stream<Arguments>> rightFn) {
        return leftFn.get().flatMap(leftArgs ->
                rightFn.get().map(rightArgs -> {
                    Collection<Object> leftObjects = new ArrayList<>(Arrays.asList(leftArgs.get()));
                    leftObjects.addAll(new ArrayList<>(Arrays.asList(rightArgs.get())));
                    return Arguments.of(leftObjects.toArray());
                }));
    }

    public static Stream<Arguments> trueFalseArguments() {
        return Stream.of(true, false).map(Arguments::of);
    }

    public static TestGraph fromGdl(String gdl) {
        return fromGdl(gdl, NATURAL, "graph");
    }

    public static TestGraph fromGdl(String gdl, String name) {
        return fromGdl(gdl, NATURAL, name);
    }

    public static TestGraph fromGdl(String gdl, Orientation orientation) {
        return fromGdl(gdl, orientation, "graph");
    }

    public static TestGraph fromGdl(String gdl, Orientation orientation, String name) {
        Objects.requireNonNull(gdl);

        var config = ImmutableGraphCreateFromGdlConfig.builder()
            .gdlGraph(gdl)
            .graphName("graph")
            .orientation(orientation)
            .build();

        var gdlFactory = GdlFactory
            .builder()
            .createConfig(config)
            .namedDatabaseId(GdlSupportExtension.DATABASE_ID)
            .build();

        return new TestGraph(gdlFactory.build().graphStore().getUnion(), gdlFactory::nodeId, name);
    }

    public static GraphStore graphStoreFromGDL(String gdl) {
        Objects.requireNonNull(gdl);
        var gdlFactory = GdlFactory.of(gdl);

        return gdlFactory.build().graphStore();
    }

    public static long[][] ids(IdFunction idFunction, String[][] variables) {
        return Arrays.stream(variables).map(vs -> ids(idFunction, vs)).toArray(long[][]::new);
    }

    public static long[] ids(IdFunction idFunction, String... variables) {
        return Arrays.stream(variables).mapToLong(idFunction::of).toArray();
    }

    public static void assertLongValues(TestGraph graph, Function<Long, Long> actualValues, Map<String, Long> expectedValues) {
        expectedValues.forEach((variable, expectedValue) -> {
            Long actualValue = actualValues.apply(graph.toMappedNodeId(variable));
            assertEquals(
                expectedValue,
                actualValue,
                formatWithLocale(
                    "Values do not match for variable %s. Expected %s, got %s.",
                    variable,
                    expectedValue.toString(),
                    actualValue.toString()
                ));
        });
    }

    public static void assertDoubleValues(TestGraph graph, Function<Long, Double> actualValues, Map<String, Double> expectedValues, double delta) {
        expectedValues.forEach((variable, expectedValue) -> {
            Double actualValue = actualValues.apply(graph.toMappedNodeId(variable));
            assertEquals(
                expectedValue,
                actualValue,
                delta,
                formatWithLocale(
                    "Values do not match for variable %s. Expected %s, got %s.",
                    variable,
                    expectedValue.toString(),
                    actualValue.toString()
                ));
        });
    }


    public static void assertGraphEquals(Graph expected, Graph actual) {
        Assertions.assertEquals(expected.nodeCount(), actual.nodeCount(), "Node counts do not match.");
        // TODO: we cannot check this right now, because the relationshhip counts depends on how the graph has been loaded for HugeGraph
//        Assertions.assertEquals(expected.relationshipCount(), actual.relationshipCount(), "Relationship counts to not match.");
        Assertions.assertEquals(CanonicalAdjacencyMatrix.canonicalize(expected), CanonicalAdjacencyMatrix.canonicalize(actual));
    }

    /**
     * Checks if exactly one of the given expected graphs matches the actual graph.
     */
    public static void assertGraphEquals(Collection<Graph> expectedGraphs, Graph actual) {
        List<String> expectedCanonicalized = expectedGraphs.stream().map(CanonicalAdjacencyMatrix::canonicalize).collect(Collectors.toList());
        String actualCanonicalized = CanonicalAdjacencyMatrix.canonicalize(actual);

        boolean equals = expectedCanonicalized
            .stream()
            .map(expected -> expected.equals(actualCanonicalized))
            .reduce(Boolean::logicalXor)
            .orElse(false);

        String message = formatWithLocale(
            "None of the given graphs matches the actual one.%nActual:%n%s%nExpected:%n%s",
            actualCanonicalized,
            String.join("\n\n", expectedCanonicalized)
        );

        assertTrue(equals, message);
    }

    public static void assertMemoryEstimation(
        Supplier<MemoryEstimation> actualMemoryEstimation,
        long nodeCount,
        int concurrency,
        long expectedMinBytes,
        long expectedMaxBytes
    ) {
       assertMemoryEstimation(actualMemoryEstimation, nodeCount, 0, concurrency, expectedMinBytes, expectedMaxBytes);
    }

    public static void assertMemoryEstimation(
        Supplier<MemoryEstimation> actualMemoryEstimation,
        long nodeCount,
        long relationshipCount,
        int concurrency,
        long expectedMinBytes,
        long expectedMaxBytes
    ) {
        assertMemoryEstimation(
            actualMemoryEstimation,
            GraphDimensions.of(nodeCount, relationshipCount),
            concurrency,
            expectedMinBytes,
            expectedMaxBytes
        );
    }

    public static void assertMemoryEstimation(
        Supplier<MemoryEstimation> actualMemoryEstimation,
        GraphDimensions dimensions,
        int concurrency,
        long expectedMinBytes,
        long expectedMaxBytes
    ) {
        var actual = actualMemoryEstimation.get().estimate(dimensions, concurrency).memoryUsage();

        assertEquals(expectedMinBytes, actual.min);
        assertEquals(expectedMaxBytes, actual.max);
    }

    public static void assertTransactionTermination(Executable executable) {
        TransactionTerminatedException exception = assertThrows(
            TransactionTerminatedException.class,
            executable
        );

        assertEquals(Status.Transaction.Terminated, exception.status());
    }

    public static void assertCypherResult(
        GraphDatabaseService db,
        @Language("Cypher") String query,
        List<Map<String, Object>> expected
    ) {
        assertCypherResult(db, query, emptyMap(), expected);
    }

    @SuppressWarnings("unchecked")
    public static void assertCypherResult(
        GraphDatabaseService db,
        @Language("Cypher") String query,
        Map<String, Object> queryParameters,
        List<Map<String, Object>> expected
    ) {
        runInTransaction(db, tx -> {
            var softAssertions = new SoftAssertions();
            List<Map<String, Object>> actual = new ArrayList<>();
            runQueryWithResultConsumer(db, query, queryParameters, result -> {
                result.accept(row -> {
                    Map<String, Object> _row = new HashMap<>();
                    for (String column : result.columns()) {
                        _row.put(column, row.get(column));
                    }
                    actual.add(_row);
                    return true;
                });
            });
            softAssertions.assertThat(actual)
                .withFailMessage("Different amount of rows returned for actual result (%d) than expected (%d)",
                    actual.size(),
                    expected.size()
                )
                .hasSize(expected.size());

            for (int i = 0; i < expected.size(); ++i) {
                Map<String, Object> expectedRow = expected.get(i);
                Map<String, Object> actualRow = actual.get(i);

                softAssertions.assertThat(actualRow.keySet()).containsExactlyInAnyOrderElementsOf(expectedRow.keySet());

                int rowNumber = i;
                expectedRow.forEach((key, expectedValue) -> {
                    Object actualValue = actualRow.get(key);
                    ObjectAssert<Object> assertion = softAssertions.assertThat(actualValue)
                        .withFailMessage(
                            "Different value for column '%s' of row %d (expected %s, but got %s)",
                            key,
                            rowNumber,
                            expectedValue,
                            actualValue
                        );

                    if (expectedValue instanceof Matcher) {
                        assertion.is(new HamcrestCondition<>((Matcher<Object>) expectedValue));
                    } else if (expectedValue instanceof Condition) {
                        assertion.is((Condition<Object>) expectedValue);
                    } else {
                        assertion.isEqualTo(expectedValue);
                    }
                });
            }
            softAssertions.assertAll();
        });
    }

    public static String getCypherAggregation(String aggregation, String property) {
        String cypherAggregation;
        switch (Aggregation.parse(aggregation)) {
            case SINGLE:
                cypherAggregation = "head(collect(%s))";
                break;
            case SUM:
                cypherAggregation = "sum(%s)";
                break;
            case MIN:
                cypherAggregation = "min(%s)";
                break;
            case MAX:
                cypherAggregation = "max(%s)";
                break;
            case COUNT:
                cypherAggregation = "count(%s)";
                break;
            default:
                cypherAggregation = "%s";
                break;
        }
        return formatWithLocale(cypherAggregation, property);
    }

    public static TransactionContext fullAccessTransaction(GraphDatabaseAPI api) {
        return TransactionContext.of(api, SecurityContext.AUTH_DISABLED);
    }

    public static NodeMapping nodeMapping(long nodeCount) {
        var builder = GraphFactory
            .initNodesBuilder()
            .nodeCount(nodeCount)
            .maxOriginalId(nodeCount - 1)
            .allocationTracker(AllocationTracker.empty())
            .build();

        for (long i = 0; i < nodeCount; i++) {
            builder.addNode(i);
        }

        return builder.build().nodeMapping();
    }

    public static NodeMapping nodeMapping(long[] originalIds) {
        var builder = GraphFactory
            .initNodesBuilder()
            .nodeCount(originalIds.length)
            .maxOriginalId(Arrays.stream(originalIds).max().orElse(0))
            .allocationTracker(AllocationTracker.empty())
            .build();

        Arrays.stream(originalIds).forEach(builder::addNode);

        return builder.build().nodeMapping();
    }
}
