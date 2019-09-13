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

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.graphalgo.core.loading.LoadGraphFactory;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class ProcTestBase {

    static GraphDatabaseAPI DB;

    static Stream<String> graphImplementations() {
        return Stream.of("Huge", "Kernel");
    }

    static Stream<String> loadDirections() {
        return Stream.of("BOTH", "INCOMING", "OUTGOING");
    }

    static Stream<Arguments> graphDirectionCombinations() {
        return graphImplementations().flatMap(impl -> loadDirections().map(direction -> arguments(impl, direction)));
    }

    @AfterAll
    static void clearLoadedGraphs() {
        LoadGraphFactory.removeAllLoadedGraphs();
    }

    protected void runQuery(String query) {
        runQuery(query, row -> {});
    }

    protected void runQuery(String query, Map<String, Object> params) {
        runQuery(query, params, row -> {});
    }

    protected void runQuery(String query, Consumer<Result.ResultRow> check) {
        runQuery(query, Collections.emptyMap(), check);
    }

    protected void runQuery(String query, GraphDatabaseAPI db) {
        runQuery(query, db, Collections.emptyMap(), row -> {});
    }

    protected void runQuery(String query, GraphDatabaseAPI db, Map<String, Object> params) {
        runQuery(query, db, params, row -> {});
    }

    protected void runQuery(String query, GraphDatabaseAPI db, Consumer<Result.ResultRow> check) {
        runQuery(query, db, Collections.emptyMap(), check);
    }

    protected static void runQuery(
            String query,
            Map<String, Object> params,
            Consumer<Result.ResultRow> check) {
        try (Result result = DB.execute(query, params)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }

    protected static void runQuery(
            String query,
            GraphDatabaseAPI db,
            Map<String, Object> params,
            Consumer<Result.ResultRow> check) {
        try (Result result = db.execute(query, params)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }

    protected static void assertEmptyResult(
            String query,
            GraphDatabaseAPI db) {
        assertEmptyResult(query, db, Collections.emptyMap());
    }

    protected static void assertEmptyResult(
            String query,
            GraphDatabaseAPI db,
            Map<String, Object> params) {
        List<Result.ResultRow> actual = new ArrayList<>();
        runQuery(query, db, params, actual::add);
        Assertions.assertTrue(actual.isEmpty());
    }

    protected static void assertMapEquals(
            Map<Long, Double> expected,
            Map<Long, Double> actual) {
        assertEquals("number of elements", expected.size(), actual.size());
        HashSet<Long> expectedKeys = new HashSet<>(expected.keySet());
        for (Map.Entry<Long, Double> entry : actual.entrySet()) {
            assertTrue(
                    "unknown key " + entry.getKey(),
                    expectedKeys.remove(entry.getKey()));
            assertEquals(
                    "value for " + entry.getKey(),
                    expected.get(entry.getKey()),
                    entry.getValue(),
                    0.1);
        }
        for (Long expectedKey : expectedKeys) {
            fail("missing key " + expectedKey);
        }
    }

    protected static void assertMapContains(IntIntMap map, int... values) {
        assertEquals("set count does not match", values.length, map.size());
        for (int count : values) {
            assertTrue("set size " + count + " does not match", mapContainsValue(map, count));
        }
    }

    private static boolean mapContainsValue(IntIntMap map, int value) {
        for (IntIntCursor cursor : map) {
            if (cursor.value == value) {
                return true;
            }
        }
        return false;
    }

    protected void assertResult(final String scoreProperty, Map<Long, Double> expected) {
        try (Transaction tx = DB.beginTx()) {
            for (Map.Entry<Long, Double> entry : expected.entrySet()) {
                double score = ((Number) DB
                        .getNodeById(entry.getKey())
                        .getProperty(scoreProperty)).doubleValue();
                assertEquals(
                        "score for " + entry.getKey(),
                        entry.getValue(),
                        score,
                        0.1);
            }
            tx.success();
        }
    }
}
