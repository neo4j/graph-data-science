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
package org.neo4j.graphalgo.doc;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.ast.Cell;
import org.asciidoctor.ast.Row;
import org.hamcrest.Matcher;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.doc.QueryConsumingTreeProcessor.QueryExampleConsumer;
import org.neo4j.graphalgo.doc.QueryConsumingTreeProcessor.SetupQueryConsumer;
import org.neo4j.graphalgo.doc.QueryConsumingTreeProcessor.Testable;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static org.asciidoctor.Asciidoctor.Factory.create;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DocTestBase extends BaseProcTest {

    static final Path ASCIIDOC_PATH = Paths.get("asciidoc");
    protected final Asciidoctor asciidoctor = create();

    protected QueryExampleConsumer otherQueryExampleConsumer() {
        return (query, columns, rows) -> assertCypherResult(query, Testable.of(columns, rows).toMaps());
    }

    protected QueryExampleConsumer defaultQueryExampleConsumer() {
        return (query, expectedColumns, expectedRows) -> {
            Result result = runQueryWithoutClosing(query, Collections.emptyMap());
            assertEquals(expectedColumns, result.columns());
            AtomicInteger index = new AtomicInteger(0);
            result.accept(actualRow -> {
                Row expectedRow = expectedRows.get(index.getAndIncrement());
                List<Cell> cells = expectedRow.getCells();
                IntStream.range(0, expectedColumns.size()).forEach(i -> {
                    String expected = cells.get(i).getText();
                    String actual = valueToString(actualRow.get(expectedColumns.get(i)));
                    assertEquals(expected, actual);
                });
                return true;
            });
            result.close();
        };
    }

    protected void assertCypherResult(String query, List<Map<String, Object>> expected) {
        try (Transaction tx = db.beginTx()) {
            List<Map<String, Object>> actual = new ArrayList<>();
            runQueryWithResultConsumer(query, result -> {
                result.accept(row -> {
                    Map<String, Object> _row = new HashMap<>();
                    for (String column : result.columns()) {
                        _row.put(column, valueToString(row.get(column)));
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
        }
    }
    protected SetupQueryConsumer defaultSetupQueryConsumer() {
        return setupQueries -> {
            assertEquals(1, setupQueries.size(), "Expected exactly one setup query");
            runQuery(setupQueries.get(0));
        };
    }

    protected String valueToString(Object value) {
        // TODO: Do we want to use the Values API here? We would get single-quote strings instead of double quotes.
        // return Values.of(value).prettyPrint();
        return value instanceof String ? "\"" + value.toString() + "\"" : value.toString();
    }

}
