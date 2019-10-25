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
import org.neo4j.graphalgo.core.loading.GraphLoadFactory;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.DependencyResolver.SelectionStrategy.ONLY;

public class ProcTestBase {

    GraphDatabaseAPI db;

    @AfterAll
    static void clearLoadedGraphs() {
        GraphLoadFactory.removeAllLoadedGraphs();
    }

    void registerFunctions(Class<?>... functionClasses) throws KernelException {
        final Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class, ONLY);
        for (Class<?> clazz : functionClasses) {
            procedures.registerFunction(clazz);
        }
    }

    void registerProcedures(Class<?>... procedureClasses) throws KernelException {
        final Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class, ONLY);
        for (Class<?> clazz : procedureClasses) {
            procedures.registerProcedure(clazz);
        }
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

    protected void runQuery(String query, GraphDatabaseAPI db, Consumer<Result.ResultRow> check) {
        runQuery(query, db, Collections.emptyMap(), check);
    }

    protected void runQuery(String query, Map<String, Object> params, Consumer<Result.ResultRow> check) {
        try (Result result = db.execute(query, params)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }

    protected void runQuery(
        String query,
        GraphDatabaseAPI db,
        Map<String, Object> params,
        Consumer<Result.ResultRow> check
    ) {
        try (Result result = db.execute(query, params)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }

    protected void assertEmptyResult(String query) {
        assertEmptyResult(query, Collections.emptyMap());
    }

    protected void assertEmptyResult(String query, Map<String, Object> params) {
        List<Result.ResultRow> actual = new ArrayList<>();
        runQuery(query, params, actual::add);
        assertTrue(actual.isEmpty());
    }

    protected void assertMapEquals(Map<Long, Double> expected, Map<Long, Double> actual) {
        assertEquals(expected.size(), actual.size(), "number of elements");
        HashSet<Long> expectedKeys = new HashSet<>(expected.keySet());
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
        try (Transaction tx = db.beginTx()) {
            for (Map.Entry<Long, Double> entry : expected.entrySet()) {
                double score = ((Number) db
                    .getNodeById(entry.getKey())
                    .getProperty(scoreProperty)).doubleValue();
                assertEquals(
                    entry.getValue(),
                    score,
                    0.1,
                    "score for " + entry.getKey()
                );
            }
            tx.success();
        }
    }

    void assertCypherResult(String query, List<Map<String, Object>> expected) {
        try (Transaction tx = db.beginTx()) {
            final List<Map<String, Object>> actual = new ArrayList<>();
            final Result result = db.execute(query);
            result.accept(row -> {
                final Map<String, Object> _row = new HashMap<>();
                for (String column : result.columns()) {
                    _row.put(column, row.get(column));
                }
                actual.add(_row);
                return true;
            });
            String reason = format(
                "Different amount of rows returned for actual result (%d) than expected (%d)",
                actual.size(),
                expected.size()
            );
            assertThat(reason, actual.size(), equalTo(expected.size()));
            for (int i = 0; i < expected.size(); ++i) {
                final Map<String, Object> expectedRow = expected.get(i);
                final Map<String, Object> actualRow = actual.get(i);
                assertThat(actualRow, equalTo(expectedRow));
            }
        }
    }
}
