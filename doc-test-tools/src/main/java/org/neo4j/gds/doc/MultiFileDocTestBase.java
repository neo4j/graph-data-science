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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.QueryRunner;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.doc.syntax.DocQuery;
import org.neo4j.gds.settings.Neo4jSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.registerFunctions;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.registerProcedures;

public abstract class MultiFileDocTestBase {
    private List<DocQuery> beforeEachQueries;

    private List<DocQuery> beforeAllQueries;

    private List<QueryExampleGroup> queryExampleGroups;

    @Inject
    protected DatabaseManagementService dbms;

    protected GraphDatabaseService defaultDb;
    protected GraphDatabaseService systemDb;

    protected abstract List<String> adocPaths();

    @BeforeEach
    protected void setUp(@TempDir File workingDirectory) throws Exception {
        this.dbms = setupDbms(workingDirectory.toPath());
        this.defaultDb = dbms.database("neo4j");
        this.systemDb = dbms.database("system");

        Class<?>[] clazzArray = new Class<?>[0];
        registerProcedures(defaultDb, procedures().toArray(clazzArray));
        registerFunctions(defaultDb, functions().toArray(clazzArray));

        for (CallableUserAggregationFunction func : aggregationFunctions()) {
            GraphDatabaseApiProxy.register(defaultDb, func);
        }

        var parseResult = new DocExampleQueryParser(workingDirectory, adocPaths())
            .parseAndCollect();

        // now that the asciidoc has been processed we can extract the queries we need
        beforeEachQueries = parseResult.beforeEachQueries();
        queryExampleGroups = parseResult.queryExampleGroups();
        beforeAllQueries = parseResult.beforeAllQueries();

        if (!setupNeo4jGraphPerTest()) {
            beforeAllQueries.forEach(this::runDocQuery);
        }
    }

    @AfterEach
    void tearDownDbms() {
        dbms.shutdown();
    }

    protected DatabaseManagementService setupDbms(Path workingDirectory) {
        var builder = new TestDatabaseManagementServiceBuilder(Neo4jLayout.of(workingDirectory));
        configureDbms(builder);
        return builder.build();
    }

    protected void configureDbms(TestDatabaseManagementServiceBuilder builder) {
        builder.noOpSystemGraphInitializer();
        builder.setConfig(Neo4jSettings.procedureUnrestricted(), singletonList("gds.*"));
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
        try {
            if (docQuery.runAsOperator()) {
                QueryRunner.runQuery(
                    dbms.database(docQuery.database()),
                    docQuery.operator(),
                    docQuery.query(),
                    Map.of()
                );
            } else {
                QueryRunner.runQuery(
                    dbms.database(docQuery.database()),
                    docQuery.operator(),
                    docQuery.query(),
                    Map.of()
                );
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to run query: " + docQuery.query(), e);
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
        if (queryExample.runAsOperator()) {
            QueryRunner.runQueryWithResultConsumer(
                dbms.database(queryExample.database()),
                queryExample.operator(),
                queryExample.query(),
                Map.of(),
                check
            );
        } else {
            QueryRunner.runQueryWithResultConsumer(
                dbms.database(queryExample.database()),
                queryExample.query(),
                Map.of(),
                check
            );
        }
    }

    private void runQueryExampleAndAssertResults(QueryExample queryExample) {
        runQueryExampleWithResultConsumer(queryExample, result -> new QueryResultValidator(
            queryExample.query(),
            queryExample.resultColumns(),
            queryExample.results(),
            result.columns(),
            result.stream().toList(),
            OptionalInt.of(getMaxFloatPrecision())
        ).validate());
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

    protected List<CallableUserAggregationFunction> aggregationFunctions() {
        return List.of();
    }

    Runnable cleanup() {
        return GraphStoreCatalog::removeAllLoadedGraphs;
    }

    protected int getMaxFloatPrecision() {
        return DocumentationTestToolsConstants.FLOAT_MAXIMUM_PRECISION;
    }
}
