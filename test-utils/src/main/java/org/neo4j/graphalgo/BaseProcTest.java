/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.hamcrest.Matcher;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphalgo.core.ExceptionMessageMatcher.containsMessage;
import static org.neo4j.graphalgo.core.ExceptionMessageMatcher.containsMessageRegex;

public class BaseProcTest {

    protected GraphDatabaseAPI db;

    @AfterAll
    static void clearLoadedGraphs() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    protected void registerFunctions(Class<?>... functionClasses) throws Exception {
        GraphDatabaseApiProxy.registerFunctions(db, functionClasses);
    }

    protected void registerAggregationFunctions(Class<?>... functionClasses) throws Exception {
        GraphDatabaseApiProxy.registerAggregationFunctions(db, functionClasses);
    }

    protected void registerProcedures(Class<?>... procedureClasses) throws Exception {
        registerProcedures(db, procedureClasses);
    }

    protected void registerProcedures(GraphDatabaseAPI db, Class<?>... procedureClasses) throws Exception {
        GraphDatabaseApiProxy.registerProcedures(db, procedureClasses);
    }

    <T> T resolveDependency(Class<T> dependency) {
        return GraphDatabaseApiProxy.resolveDependency(db, dependency);
    }

    protected String getUsername() {
        return AuthSubject.ANONYMOUS.username();
    }

    protected void runQueryWithRowConsumer(@Language("Cypher") String query, Consumer<Result.ResultRow> check) {
        runQueryWithRowConsumer(query, emptyMap(), check);
    }

    protected void runQueryWithRowConsumer(
        @Language("Cypher") String query,
        Map<String, Object> params,
        Consumer<Result.ResultRow> check
    ) {
        runQueryWithRowConsumer(db, query, params, check);
    }

    protected void runQueryWithRowConsumer(
        GraphDatabaseAPI localDb,
        @Language("Cypher") String query,
        Map<String, Object> params,
        Consumer<Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(localDb, query, params, check);
    }

    protected void runQueryWithRowConsumer(
        GraphDatabaseAPI localDb,
        @Language("Cypher") String query,
        Consumer<Result.ResultRow> check
    ) {
        runQueryWithRowConsumer(localDb, query, emptyMap(), check);
    }

    protected void runQueryWithRowConsumer(
        String username,
        String query,
        Consumer<Result.ResultRow> check
    ) {
        runQueryWithRowConsumer(db, username, query, emptyMap(), check);
    }

    protected void runQueryWithRowConsumer(
        GraphDatabaseAPI db,
        String username,
        String query,
        Map<String, Object> params,
        Consumer<Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(db, username, query, params, check);
    }

    protected void runQuery(String username, String query, Map<String, Object> params) {
        QueryRunner.runQuery(db, username, query, params);
    }

    protected void runQuery(GraphDatabaseAPI db, String query, Map<String, Object> params) {
        QueryRunner.runQuery(db, query, params);
    }

    protected void runQuery(String query) {
        QueryRunner.runQuery(db, query);
    }

    protected void runQuery(String query, Map<String, Object> params) {
        QueryRunner.runQuery(db, query, params);
    }

    protected <T> T runQuery(String query, Function<Result, T> resultFunction) {
        return runQuery(query, emptyMap(), resultFunction);
    }

    protected <T> T runQuery(String query, Map<String, Object> params, Function<Result, T> resultFunction) {
        return runQuery(db, query, params, resultFunction);
    }

    protected <T> T runQuery(GraphDatabaseAPI db, String query, Map<String, Object> params, Function<Result, T> resultFunction) {
        return QueryRunner.runQuery(db, query, params, resultFunction);
    }

    protected void runQueryWithResultConsumer(@Language("Cypher") String query, Map<String, Object> params, Consumer<Result> check) {
        QueryRunner.runQueryWithResultConsumer(
            db,
            query,
            params,
            check
        );
    }
    protected void runQueryWithResultConsumer(@Language("Cypher") String query, Consumer<Result> check) {
        runQueryWithResultConsumer(
            query,
            emptyMap(),
            check
        );
    }

    /**
     * This is to be used with caution;
     * Callers have to consume the Result and/or use try-catch resource block.
     */
    protected Result runQueryWithoutClosing(String query, Map<String, Object> params) {
        return QueryRunner.runQueryWithoutClosing(db, query, params);
    }

    protected void assertEmptyResult(String query) {
        assertEmptyResult(query, emptyMap());
    }

    private void assertEmptyResult(String query, Map<String, Object> params) {
        List<Result.ResultRow> actual = new ArrayList<>();
        QueryRunner.runQueryWithRowConsumer(db, query, params, actual::add);
        assertTrue(actual.isEmpty());
    }

    protected void assertMapEquals(Map<Long, Double> expected, Map<Long, Double> actual) {
        assertEquals(expected.size(), actual.size(), "number of elements");
        Collection<Long> expectedKeys = new HashSet<>(expected.keySet());
        for (Map.Entry<Long, Double> entry : actual.entrySet()) {
            assertTrue(
                expectedKeys.remove(entry.getKey()),
                "unknown key " + entry.getKey()
            );
            assertEquals(
                expected.get(entry.getKey()),
                entry.getValue(),
                0.1,
                "value for " + entry.getKey()
            );
        }
        for (Long expectedKey : expectedKeys) {
            fail("missing key " + expectedKey);
        }
    }

    protected void assertMapContains(IntIntMap map, int... values) {
        assertEquals(values.length, map.size(), "set count does not match");
        for (int count : values) {
            assertTrue(mapContainsValue(map, count), "set size " + count + " does not match");
        }
    }

    private boolean mapContainsValue(IntIntMap map, int value) {
        for (IntIntCursor cursor : map) {
            if (cursor.value == value) {
                return true;
            }
        }
        return false;
    }

    protected void assertResult(final String scoreProperty, Map<Long, Double> expected) {
        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            for (Map.Entry<Long, Double> entry : expected.entrySet()) {
                double score = ((Number) GraphDatabaseApiProxy.getNodeById(db, entry.getKey())
                    .getProperty(scoreProperty))
                    .doubleValue();
                assertEquals(
                    entry.getValue(),
                    score,
                    0.1,
                    "score for " + entry.getKey()
                );
            }
        });
    }

    protected void assertCypherResult(@Language("Cypher") String query, List<Map<String, Object>> expected) {
        assertCypherResult(query, emptyMap(), expected);
    }

    protected void assertCypherResult(
        @Language("Cypher") String query,
        Map<String, Object> queryParameters,
        List<Map<String, Object>> expected
    ) {
        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            List<Map<String, Object>> actual = new ArrayList<>();
            runQueryWithResultConsumer(query, queryParameters, result -> {
                result.accept(row -> {
                    Map<String, Object> _row = new HashMap<>();
                    for (String column : result.columns()) {
                        _row.put(column, row.get(column));
                    }
                    actual.add(_row);
                    return true;
                });
            });
            String reason = format(
                "Different amount of rows returned for actual result (%d) than expected (%d)",
                actual.size(),
                expected.size()
            );
            assertThat(reason, actual.size(), equalTo(expected.size()));
            for (int i = 0; i < expected.size(); ++i) {
                Map<String, Object> expectedRow = expected.get(i);
                Map<String, Object> actualRow = actual.get(i);

                assertThat(actualRow.keySet(), equalTo(expectedRow.keySet()));
                int rowNumber = i;
                expectedRow.forEach((key, expectedValue) -> {
                    Matcher<Object> matcher;
                    if (expectedValue instanceof Matcher) {
                        //noinspection unchecked
                        matcher = (Matcher<Object>) expectedValue;
                    } else {
                        matcher = equalTo(expectedValue);
                    }
                    Object actualValue = actualRow.get(key);
                    assertThat(
                        String.format("Different value for column '%s' of row %d", key, rowNumber),
                        actualValue, matcher
                    );
                });
            }
        });
    }

    protected void assertError(
        @Language("Cypher") String query,
        String messageSubstring
    ) {
       assertError(query, emptyMap(), messageSubstring);
    }

    protected void assertError(
        @Language("Cypher") String query,
        Map<String, Object> queryParameters,
        String messageSubstring
    ) {
        assertError(query, queryParameters, containsMessage(messageSubstring));
    }

    protected void assertErrorRegex(
        @Language("Cypher") String query,
        String regex
    ) {
        assertErrorRegex(query, emptyMap(), regex);
    }

    protected void assertErrorRegex(
        @Language("Cypher") String query,
        Map<String, Object> queryParameters,
        String regex
    ) {
        assertError(query, queryParameters, containsMessageRegex(regex));
    }

    private void assertError(
        @Language("Cypher") String query,
        Map<String, Object> queryParameters,
        Matcher<Throwable> matcher
    ) {
        try {
            runQueryWithResultConsumer(query, queryParameters, this::consume);
            fail(format("Expected an exception to be thrown by query:\n%s", query));
        } catch (Throwable e) {
            assertThat(e, matcher);
        }
    }

    private void consume(Result result) {
        while (result.hasNext()) {
            result.next();
        }
        result.close();
    }

    protected void assertUserInput(Result.ResultRow row, String key, Object expected) {
        Map<String, Object> configMap = extractUserInput(row);
        assertTrue(configMap.containsKey(key), String.format("Key %s is not present in config", key));
        assertEquals(expected, configMap.get(key));
    }

    private Map<String, Object> extractUserInput(Result.ResultRow row) {
        return ((Map<String, Object>) row.get("configuration"));
    }

    protected void assertGraphExists(String graphName) {
        final Set<Graph> graphs = getLoadedGraphs(graphName);

        assertEquals(1, graphs.size());
    }

    protected void assertGraphDoesNotExist(String graphName) {
        final Set<Graph> graphs = getLoadedGraphs(graphName);
        assertTrue(graphs.isEmpty());
    }

    protected Graph findLoadedGraph(String graphName) {
        return GraphStoreCatalog
            .getLoadedGraphs("")
            .entrySet()
            .stream()
            .filter(e -> e.getKey().graphName().equals(graphName))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(() -> new RuntimeException(String.format("Graph %s not found.", graphName)));
    }

    private Set<Graph> getLoadedGraphs(String graphName) {
        return GraphStoreCatalog
                .getLoadedGraphs("")
                .entrySet()
                .stream()
                .filter(e -> e.getKey().graphName().equals(graphName))
                .map(Map.Entry::getValue)
                .collect(Collectors.toSet());
    }
}
