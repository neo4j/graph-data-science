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
package org.neo4j.graphalgo.doc;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.ast.Cell;
import org.asciidoctor.ast.Row;
import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.doc.QueryConsumingTreeProcessor.QueryExampleConsumer;
import org.neo4j.graphalgo.doc.QueryConsumingTreeProcessor.SetupQueryConsumer;
import org.neo4j.graphalgo.functions.AsNodeFunc;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.values.storable.Values;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.asciidoctor.Asciidoctor.Factory.create;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.runInTransaction;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.runQueryWithoutClosingTheResult;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.utils.StringJoining.joinInGivenOrder;

abstract class DocTestBase extends BaseProcTest {

    private static final Path ASCIIDOC_PATH = Paths.get("asciidoc");
    private static final String DELIMITER = " | ";

    final Asciidoctor asciidoctor = create();

    boolean printActuals() {
        return false;
    }

    QueryConsumingTreeProcessor defaultTreeProcessor() {
        return new QueryConsumingTreeProcessor(
            defaultSetupQueryConsumer(),
            defaultQueryExampleConsumer(),
            defaultQueryExampleNoResultConsumer(),
            this.cleanup()
        );
    }

    @BeforeEach
    void setUp() throws Exception {
        Class<?>[] clazzArray = new Class<?>[0];
        registerProcedures(procedures().toArray(clazzArray));
        registerFunctions(functions().toArray(clazzArray));
    }

    @AfterAll
    static void clearLoadedGraphs() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    Runnable cleanup() {
        return GraphStoreCatalog::removeAllLoadedGraphs;
    }

    abstract List<Class<?>> procedures();

    abstract String adocFile();

    List<Class<?>> functions() {
        return Collections.singletonList(AsNodeFunc.class);
    }

    @Test
    void dontPrintActuals() {
        assertFalse(printActuals(), "This should only be true in development, never committed!");
    }

    @Test
    void runTest() throws URISyntaxException {
        asciidoctor.javaExtensionRegistry().treeprocessor(defaultTreeProcessor());
        File file = ASCIIDOC_PATH.resolve(adocFile()).toFile();
        assertTrue(
            file.exists() && file.canRead(),
            formatWithLocale("File %s doesn't exist or can't be read", file.getPath())
        );
        asciidoctor.loadFile(file, Collections.emptyMap());
    }

    private SetupQueryConsumer defaultSetupQueryConsumer() {
        return setupQueries -> {
            setupQueries.forEach(this::runQuery);
        };
    }

    private QueryExampleConsumer defaultQueryExampleConsumer() {
        return (query, expectedColumns, expectedRows) -> {
            runInTransaction(db, tx -> {
                try (Result result = runQueryWithoutClosingTheResult(tx, query, Collections.emptyMap())) {
                    if (printActuals()) {
                        System.out.println(DELIMITER + joinInGivenOrder(result.columns().stream(), DELIMITER));
                        result.forEachRemaining(row -> System.out.println(DELIMITER + joinInGivenOrder(
                            result.columns().stream().map(row::get).map(this::valueToString),
                            DELIMITER
                        )));
                    }
                    assertEquals(
                        expectedColumns,
                        result.columns(),
                        "Expected columns were different from actual: " + query
                    );
                    AtomicInteger index = new AtomicInteger(0);
                    result.accept(actualRow -> {
                        Row expectedRow = expectedRows.get(index.getAndIncrement());
                        List<Cell> cells = expectedRow.getCells();
                        IntStream.range(0, expectedColumns.size()).forEach(i -> {
                            String expected = cells.get(i).getText();
                            String actual = valueToString(actualRow.get(expectedColumns.get(i)));
                            assertEquals(expected, actual, formatWithLocale("Query: %s", query));
                        });
                        return true;
                    });
                } catch (QueryExecutionException e) {
                    fail("query failed: " + query, e);
                }
            });
        };
    }

    private QueryConsumingTreeProcessor.QueryExampleNoResultConsumer defaultQueryExampleNoResultConsumer() {
        return (query) -> {
            try {
                runQuery(query);
            } catch (QueryExecutionException e) {
                fail("query failed: " + query, e);
            }
        };
    }

    private String valueToString(Object value) {
        // TODO: Do we want to use the Values API here? We would get single-quote strings instead of double quotes.
        // return Values.of(value).prettyPrint();
        if (value == null) {
            return "null";
        } else if (value instanceof String) {
            return "\"" + value.toString() + "\"";
        } else if (Arrays.isArray(value)) {
            return Values.of(value).prettyPrint();
        } else {
            return value.toString();
        }
    }
}
