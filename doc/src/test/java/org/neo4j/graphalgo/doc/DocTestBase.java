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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.functions.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.doc.QueryConsumingTreeProcessor.QueryExampleConsumer;
import org.neo4j.graphalgo.doc.QueryConsumingTreeProcessor.SetupQueryConsumer;
import org.neo4j.graphdb.Result;

import java.io.File;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.runInTransaction;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.runQueryWithoutClosingTheResult;

abstract class DocTestBase extends BaseProcTest {

    private static final Path ASCIIDOC_PATH = Paths.get("asciidoc");

    final Asciidoctor asciidoctor = create();

    QueryConsumingTreeProcessor defaultTreeProcessor() {
        return new QueryConsumingTreeProcessor(
            defaultSetupQueryConsumer(),
            defaultQueryExampleConsumer(),
            defaultQueryExampleNoResultConsumer()
        );
    }

    @BeforeEach
    void setUp() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        Class<?>[] clazzArray = new Class<?>[0];
        registerProcedures(procedures().toArray(clazzArray));
        registerFunctions(functions().toArray(clazzArray));
    }

    abstract List<Class<?>> procedures();

    abstract String adocFile();

    List<Class<?>> functions() {
        return Collections.singletonList(GetNodeFunc.class);
    }

    @Test
    void runTest() {
        asciidoctor.javaExtensionRegistry().treeprocessor(defaultTreeProcessor());
        File file = ASCIIDOC_PATH.resolve(adocFile()).toFile();
        assertTrue(file.exists() && file.canRead());
        asciidoctor.loadFile(file, Collections.emptyMap());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void assertCypherResult(String query, List<Map<String, Object>> expected) {
        runInTransaction(db, tx -> {
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

    private SetupQueryConsumer defaultSetupQueryConsumer() {
        return setupQueries -> {
            setupQueries.forEach(this::runQuery);
        };
    }

    private QueryExampleConsumer defaultQueryExampleConsumer() {
        return (query, expectedColumns, expectedRows) -> {
            runInTransaction(db, tx -> {
                try (Result result = runQueryWithoutClosingTheResult(db, tx, query, Collections.emptyMap())) {
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
                }
            });
        };
    }

    private QueryConsumingTreeProcessor.QueryExampleNoResultConsumer defaultQueryExampleNoResultConsumer() {
        return this::runQuery;
    }

    private String valueToString(Object value) {
        // TODO: Do we want to use the Values API here? We would get single-quote strings instead of double quotes.
        // return Values.of(value).prettyPrint();
        return value instanceof String
            ? "\"" + value.toString() + "\""
            : value != null ? value.toString() : "null";
    }

}
