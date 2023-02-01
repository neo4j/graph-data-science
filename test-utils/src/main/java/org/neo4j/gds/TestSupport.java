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
import org.immutables.builder.Builder;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.canonization.CanonicalAdjacencyMatrix;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.extension.GdlSupportPerMethodExtension;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.gdl.ImmutableGraphProjectFromGdlConfig;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.gds.transaction.TransactionContextImpl;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.exceptions.Status;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.REVERSE;
import static org.neo4j.gds.QueryRunner.runQueryWithResultConsumer;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runInTransaction;
import static org.neo4j.gds.utils.StringFormatting.formatNumber;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TestSupport {

    public static final boolean CI =
        System.getenv("TEAMCITY_VERSION") != null || System.getenv("CI") != null || System.getenv("BUILD_ID") != null;

    private TestSupport() {}

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
        return new GdlBuilder().gdl(gdl).build();
    }

    public static TestGraph fromGdl(String gdl, long idOffset) {
        return new GdlBuilder().gdl(gdl).idSupplier(new OffsetIdSupplier(idOffset)).build();
    }

    public static TestGraph fromGdl(String gdl, String name) {
        return new GdlBuilder().gdl(gdl).name(name).build();
    }

    public static TestGraph fromGdl(String gdl, Orientation orientation) {
        return new GdlBuilder().gdl(gdl).orientation(orientation).build();
    }

    public static TestGraph fromGdl(String gdl, Orientation orientation, String name) {
        return new GdlBuilder().gdl(gdl).orientation(orientation).name(name).build();
    }

    @Builder.Factory
    public static TestGraph gdl(
        String gdl,
        Optional<String> name,
        Optional<Orientation> orientation,
        Optional<Aggregation> aggregation,
        Optional<LongSupplier> idSupplier,
        Optional<DatabaseId> databaseId,
        Optional<Boolean> indexInverse
    ) {
        Objects.requireNonNull(gdl);

        var graphName = name.orElse("graph");

        var config = ImmutableGraphProjectFromGdlConfig.builder()
            .gdlGraph(gdl)
            .graphName(graphName)
            .orientation(orientation.orElse(NATURAL))
            .aggregation(aggregation.orElse(Aggregation.DEFAULT))
            .indexInverse(indexInverse.orElse(false))
            .build();

        var gdlFactory = GdlFactory
            .builder()
            .nodeIdFunction(idSupplier.orElse(new OffsetIdSupplier(0L)))
            .graphProjectConfig(config)
            .databaseId(databaseId.orElse(GdlSupportPerMethodExtension.DATABASE_ID))
            .build();

        return new TestGraph(gdlFactory.build().getUnion(), gdlFactory::nodeId, graphName);
    }

    public static GraphStore graphStoreFromGDL(String gdl) {
        Objects.requireNonNull(gdl);

        return GdlFactory.of(gdl).build();
    }

    @Builder.Factory
    public static GraphStore gdlGraphStore(
        String gdl,
        Optional<String> name,
        Optional<Orientation> orientation,
        Optional<Aggregation> aggregation,
        Optional<LongSupplier> idSupplier,
        Optional<DatabaseId> databaseId,
        Optional<Boolean> indexInverse
    ) {
        Objects.requireNonNull(gdl);

        var graphName = name.orElse("graph");

        var config = ImmutableGraphProjectFromGdlConfig.builder()
            .gdlGraph(gdl)
            .graphName(graphName)
            .orientation(orientation.orElse(NATURAL))
            .aggregation(aggregation.orElse(Aggregation.DEFAULT))
            .indexInverse(indexInverse.orElse(false))
            .build();

        var gdlFactory = GdlFactory
            .builder()
            .nodeIdFunction(idSupplier.orElse(new OffsetIdSupplier(0L)))
            .graphProjectConfig(config)
            .databaseId(databaseId.orElse(GdlSupportPerMethodExtension.DATABASE_ID))
            .build();

        return gdlFactory.build();
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
        Assertions.assertEquals(expected.relationshipCount(), actual.relationshipCount(), "Relationship counts do not match.");
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
        MemoryRange expected
    ) {
        assertMemoryEstimation(actualMemoryEstimation, nodeCount, 0, concurrency, expected);
    }

    public static void assertMemoryEstimation(
        Supplier<MemoryEstimation> actualMemoryEstimation,
        long nodeCount,
        long relationshipCount,
        int concurrency,
        MemoryRange expected
    ) {
        var actual = actualMemoryEstimation
            .get().estimate(GraphDimensions.of(nodeCount, relationshipCount), concurrency).memoryUsage();
        assertMemoryRange(actual, expected.min, expected.max);
    }

    public static void assertMemoryRange(MemoryRange actual, long expected) {
        assertMemoryRange(actual, expected, expected);
    }

    public static void assertMemoryRange(MemoryRange actual, MemoryRange expected) {
        assertMemoryRange(actual, expected.min, expected.max);
    }

    public static void assertMemoryRange(MemoryRange actual, long expectedMin, long expectedMax) {
        assertThat(actual)
            .withFailMessage(
                "Got (%s, %s), but expected (%s, %s)",
                formatNumber(actual.min),
                formatNumber(actual.max),
                formatNumber(expectedMin),
                formatNumber(expectedMax)
            )
            .isEqualTo(MemoryRange.of(expectedMin, expectedMax));
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

    // should be used with YIELD bytesMin, bytesMax, nodeCount, relationshipCount
    public static void assertCypherMemoryEstimation(
        GraphDatabaseService db,
        @Language("Cypher") String query,
        MemoryRange expected,
        long expectedNodeCount,
        long expectedRelationshipCount
    ) {
        assertCypherMemoryEstimation(db, query, Map.of(), expected, expectedNodeCount, expectedRelationshipCount);
    }

    // should be used with YIELD bytesMin, bytesMax, nodeCount, relationshipCount
    public static void assertCypherMemoryEstimation(
        GraphDatabaseService db,
        @Language("Cypher") String query,
        Map<String, Object> queryParameters,
        MemoryRange expected,
        long expectedNodeCount,
        long expectedRelationshipCount
    ) {
        SoftAssertions softly = new SoftAssertions();
        QueryRunner.runQueryWithRowConsumer(
            db,
            query,
            queryParameters,
            (transaction, row) -> {
                try {
                    assertMemoryRange(
                        MemoryRange.of((long) row.getNumber("bytesMin"), (long) row.getNumber("bytesMax")),
                        expected.min, expected.max
                    );
                } catch (Throwable e) {
                    softly.fail(e.getMessage());
                }
                var actualNodeCount = (long) row.getNumber("nodeCount");
                var actualRelationshipCount = (long) row.getNumber("relationshipCount");
                softly.assertThat(expectedNodeCount)
                    .withFailMessage(() -> formatWithLocale(
                        "Got nodeCount %s but expected %s",
                        formatNumber(actualNodeCount),
                        formatNumber(expectedNodeCount)
                    ))
                    .isEqualTo(actualNodeCount);
                softly.assertThat(expectedRelationshipCount)
                    .withFailMessage(() -> formatWithLocale(
                        "Got relationshipCount %s but expected %s",
                        formatNumber(actualRelationshipCount),
                        formatNumber(expectedRelationshipCount)
                    ))
                    .isEqualTo(actualRelationshipCount);
            }
        );
        softly.assertAll();
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

            assertThat(actual)
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

    public static TransactionContext fullAccessTransaction(GraphDatabaseService databaseService) {
        return TransactionContextImpl.of(databaseService, SecurityContext.AUTH_DISABLED);
    }

    public static IdMap idMap(long nodeCount) {
        var builder = GraphFactory
            .initNodesBuilder()
            .nodeCount(nodeCount)
            .maxOriginalId(nodeCount - 1)
            .build();

        for (long i = 0; i < nodeCount; i++) {
            builder.addNode(i);
        }

        return builder.build().idMap();
    }

    public static IdMap idMap(long[] originalIds) {
        var builder = GraphFactory
            .initNodesBuilder()
            .nodeCount(originalIds.length)
            .maxOriginalId(Arrays.stream(originalIds).max().orElse(0))
            .build();

        Arrays.stream(originalIds).forEach(builder::addNode);

        return builder.build().idMap();
    }

    public static class OffsetIdSupplier implements LongSupplier {
        private final AtomicLong offset;

        public OffsetIdSupplier(long offset) {
            this.offset = new AtomicLong(offset);
        }

        @Override
        public long getAsLong() {
            return offset.getAndIncrement();
        }
    }
}
