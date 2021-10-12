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
import org.asciidoctor.OptionsBuilder;
import org.asciidoctor.SafeMode;
import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.values.storable.Values;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.asciidoctor.Asciidoctor.Factory.create;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public abstract class DocTestBase extends BaseProcTest {

    @TempDir
    File workDir;

    private static final Path ASCIIDOC_PATH = Paths.get("asciidoc");
    private List<String> beforeEachQueries;
    private List<String> beforeAllQueries;
    private List<QueryExampleGroup> queryExampleGroups;

    protected abstract String adocFile();

    protected abstract List<Class<?>> procedures();

    @BeforeEach
    void setUp() throws Exception {
        Class<?>[] clazzArray = new Class<?>[0];
        registerProcedures(procedures().toArray(clazzArray));
        registerFunctions(functions().toArray(clazzArray));
        Asciidoctor asciidoctor = create();
        var treeProcessor = new QueryCollectingTreeProcessor();
        asciidoctor.javaExtensionRegistry().treeprocessor(treeProcessor);

        var docFile = ASCIIDOC_PATH.resolve(adocFile()).toFile();
        assertThat(docFile).exists().canRead();

        var options = OptionsBuilder.options()
            .toDir(workDir) // Make sure we don't write anything in the project.
            .safe(SafeMode.UNSAFE); // By default we are forced to use relative path which we don't want.

        asciidoctor.convertFile(docFile, options);

        beforeEachQueries = treeProcessor.beforeEachQueries();
        queryExampleGroups = treeProcessor.queryExamples();

        beforeAllQueries = treeProcessor.beforeAllQueries();

        if (!setupNeo4jGraphPerTest()) {
            beforeAllQueries.forEach(this::runQuery);
        }
    }

    @TestFactory
    Collection<DynamicTest> runTests() {
        return queryExampleGroups.stream()
            .map(this::createDynamicTest)
            .collect(Collectors.toList());
    }

    boolean setupNeo4jGraphPerTest() {
        return false;
    }

    // This emulates `@BeforeEach`. Typically used to project any GDS Graphs
    private void beforeEachTest() {
        if(setupNeo4jGraphPerTest()) {
            beforeAllQueries.forEach(super::runQuery);
        }
        beforeEachQueries.forEach(this::runQuery);
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

    List<Class<?>> functions() { return List.of(); }

    Runnable cleanup() {
        return GraphStoreCatalog::removeAllLoadedGraphs;
    }

    private void runQueryExample(QueryExample queryExample) {
        if(queryExample.assertResults()) {
            runQueryExampleAndAssertResults(queryExample);
        } else {
            assertThatNoException().isThrownBy(() -> runQuery(queryExample.query()));
        }
    }

    private void runQueryExampleAndAssertResults(QueryExample queryExample) {
        runQueryWithResultConsumer(queryExample.query(), result -> {
            assertThat(queryExample.resultColumns()).containsExactlyElementsOf(result.columns());

            var actualResults = new ArrayList<List<String>>();

            while (result.hasNext()) {
                var actualResultRow = result.next();
                var actualResultValues = queryExample.resultColumns().stream()
                    .map(column -> valueToString(actualResultRow.get(column)))
                    .collect(Collectors.toList());
                actualResults.add(actualResultValues);
            }
            assertThat(actualResults)
                .as(queryExample.query())
                .containsExactlyElementsOf(queryExample.results());
        });
    }

    private String valueToString(Object value) {
        // TODO: Do we want to use the Values API here? We would get single-quote strings instead of double quotes.
        // return Values.of(value).prettyPrint();
        if (value == null) {
            return "null";
        } else if (value instanceof String) {
            return "\"" + value + "\"";
        } else if (Arrays.isArray(value)) {
            return Values.of(value).prettyPrint();
        } else {
            return value.toString();
        }
    }
}
