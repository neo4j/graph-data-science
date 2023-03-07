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
package org.neo4j.gds.doc;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.Options;
import org.asciidoctor.SafeMode;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.compat.CompatUserAggregationFunction;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.doc.syntax.DocQuery;
import org.neo4j.graphdb.Result;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public abstract class MultiFileDocTestBase extends BaseProcTest {
    private List<DocQuery> beforeEachQueries;

    private List<DocQuery> beforeAllQueries;

    private List<QueryExampleGroup> queryExampleGroups;

    protected abstract List<String> adocPaths();

    @BeforeEach
    void setUp(@TempDir File workingDirectory) throws Exception {
        Class<?>[] clazzArray = new Class<?>[0];
        registerProcedures(procedures().toArray(clazzArray));
        registerFunctions(functions().toArray(clazzArray));
        for (var function : aggregationFunctions()) {
            registerAggregationFunction(function);
        }

        var treeProcessor = new QueryCollectingTreeProcessor();
        var includeProcessor = new PartialsIncludeProcessor();

        try (var asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.javaExtensionRegistry()
                .includeProcessor(includeProcessor)
                .treeprocessor(treeProcessor);

            var options = Options.builder()
                .toDir(workingDirectory) // Make sure we don't write anything in the project.
                .safe(SafeMode.UNSAFE); // By default, we are forced to use relative path which we don't want.

            for (var docFile : adocFiles()) {
                asciidoctor.convertFile(docFile, options.build());
            }
        }

        // now that the asciidoc has been processed we can extract the queries we need
        beforeEachQueries = treeProcessor.getBeforeEachQueries();
        queryExampleGroups = treeProcessor.getQueryExampleGroups();
        beforeAllQueries = treeProcessor.getBeforeAllQueries();

        if (!setupNeo4jGraphPerTest()) {
            beforeAllQueries.forEach(this::runDocQuery);
        }
    }

    private List<File> adocFiles() {
        return adocPaths()
            .stream()
            .map(DocumentationTestToolsConstants.ASCIIDOC_PATH::resolve)
            .map(Path::toFile)
            .collect(Collectors.toList());
    }

    @TestFactory
    Collection<DynamicTest> runTests() {
        Assertions.assertThat(queryExampleGroups)
            .as("Query Example Groups should not be empty!")
            .isNotEmpty();

        return queryExampleGroups.stream()
            .map(this::createDynamicTest)
            .collect(Collectors.toList());
    }

    boolean setupNeo4jGraphPerTest() {
        return false;
    }

    // This emulates `@BeforeEach`. Typically used to project any GDS Graphs
    private void beforeEachTest() {
        if (setupNeo4jGraphPerTest()) {
            beforeAllQueries.forEach(this::runDocQuery);
        }
        beforeEachQueries.forEach(this::runDocQuery);
    }

    private void runDocQuery(DocQuery docQuery) {
        if (docQuery.runAsOperator()) {
            String operatorHandle = docQuery.operator();
            super.runQuery(operatorHandle, docQuery.query());
        } else {
            super.runQuery(docQuery.query());
        }
    }

    private DynamicTest createDynamicTest(QueryExampleGroup queryExampleGroup) {
        return dynamicTest(queryExampleGroup.displayName(), () -> {
            try {
                beforeEachTest();

                // This is the actual test
                queryExampleGroup.queryExamples().forEach(this::runQueryExample);
            } finally {
                // This emulates `@AfterEach`
                cleanup().run();
            }
        });
    }

    private void runQueryExampleWithResultConsumer(QueryExample queryExample, Consumer<Result> check) {
        if (!queryExample.runAsOperator()) {
            super.runQueryWithResultConsumer(queryExample.query(), check);
        } else {
            String operatorHandle = queryExample.operator();
            super.runQueryWithResultConsumer(operatorHandle, queryExample.query(), check);
        }
    }

    private void runQueryExampleAndAssertResults(QueryExample queryExample) {
        runQueryExampleWithResultConsumer(queryExample, result -> {
            assertThat(queryExample.resultColumns()).containsExactlyElementsOf(result.columns());

            var actualResults = new ArrayList<List<String>>();

            while (result.hasNext()) {
                var actualResultRow = result.next();
                var actualResultValues = queryExample.resultColumns().stream()
                    .map(column -> valueToString(actualResultRow.get(column)))
                    .collect(Collectors.toList());
                actualResults.add(actualResultValues);
            }
            var expectedResults = reducePrecisionOfDoubles(queryExample.results());
            assertThat(actualResults)
                .as(queryExample.query())
                .containsExactlyElementsOf(expectedResults);
        });
    }

    private void runQueryExample(QueryExample queryExample) {
        if (queryExample.assertResults()) {
            runQueryExampleAndAssertResults(queryExample);
        } else {
            assertThatNoException().isThrownBy(() -> runDocQuery(queryExample));
        }
    }

    protected abstract List<Class<?>> procedures();

    protected List<Class<?>> functions() {
        return List.of();
    }

    protected List<CompatUserAggregationFunction> aggregationFunctions() {
        return List.of();
    }

    Runnable cleanup() {
        return GraphStoreCatalog::removeAllLoadedGraphs;
    }

    private List<List<String>> reducePrecisionOfDoubles(Collection<List<String>> resultsFromDoc) {
        return resultsFromDoc
            .stream()
            .map(list -> list.stream().map(string -> {
                    try {
                        if (string.startsWith("[")) {
                            return formatListOfNumbers(string);
                        }
                        return DocumentationTestToolsConstants.FLOAT_FORMAT.format(Double.parseDouble(string));
                    } catch (NumberFormatException e) {
                        return string;
                    }
                }).collect(Collectors.toList())
            ).collect(Collectors.toList());
    }

    @NotNull
    private static String formatListOfNumbers(String string) {
        var withoutBrackets = string.substring(1, string.length() - 1);
        var commaSeparator = ", ";
        var parts = withoutBrackets.split(commaSeparator);
        var builder = new StringBuilder("[");
        var separator = "";
        for (var part : parts) {
            builder.append(separator);
            builder.append(DocumentationTestToolsConstants.FLOAT_FORMAT.format(Double.parseDouble(part)));
            separator = commaSeparator;
        }
        builder.append("]");

        return builder.toString();
    }

    private String valueToString(Object value) {
        if (value == null) {
            return "null";
        } else if (value instanceof String) {
            return "\"" + value + "\"";
        } else if (value instanceof Double) {
            return DocumentationTestToolsConstants.FLOAT_FORMAT.format(value);
        } else if (value instanceof List<?>) {
            return ((List<?>) value).stream().map(v -> {
                if (v instanceof Number) {
                    return DocumentationTestToolsConstants.FLOAT_FORMAT.format(v);
                }
                return v;
            }).collect(Collectors.toList()).toString();
        } else if (value instanceof Map<?, ?>) {
            var map = ((Map<?, ?>) value);
            var mappedMap = map.entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().toString(),
                entry -> {
                    var innerValue = entry.getValue();
                    if (innerValue instanceof Map<?, ?>) {
                        return new TreeMap<>((Map<?, ?>) innerValue);
                    }
                    return innerValue;
                }
            ));
            return new TreeMap<>(mappedMap).toString();
        } else {
            return value.toString();
        }
    }
}
