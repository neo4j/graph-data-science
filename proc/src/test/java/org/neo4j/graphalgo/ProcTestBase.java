package org.neo4j.graphalgo;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProcTestBase {
    static GraphDatabaseAPI db;

    static Stream<String> graphImplementations() {
        return Stream.of("Heavy", "Huge", "Kernel");
    }

    protected static void runQuery(
            String query,
            Map<String, Object> params,
            Consumer<Result.ResultRow> check) {
        try (Result result = db.execute(query, params)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
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

    protected void assertResult(final String scoreProperty, Map<Long, Double> expected) {
        try (Transaction tx = db.beginTx()) {
            for (Map.Entry<Long, Double> entry : expected.entrySet()) {
                double score = ((Number) db
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
