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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.test.TestProc;
import org.neo4j.graphdb.QueryExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.gds.ThrowableRootCauseMatcher.rootCause;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ConfigKeyValidationTest extends BaseProcTest {

    private static final String newLine = System.lineSeparator();

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, TestProc.class);
    }

    @Test
    void additionalKeyForGraphProject() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.graph.project('foo', '*', '*', {readConcurrency: 4, maxIterations: 1337})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key: maxIterations")
        );
    }

    @Test
    void additionalKeyForExplicitLoading() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.testProc.write('foo', {nodeProjection: '*', writeProperty: 'p'})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key: nodeProjection")
        );
    }

    @Test
    void misspelledRequiredKeyWithSuggestion() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.testProc.write('g', {wirteProperty: 'foo'})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "No value specified for the mandatory configuration parameter `writeProperty` (a similar parameter exists: [wirteProperty])")
        );
    }

    @Test
    void misspelledOptionalKeyWithSuggestion() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.testProc.write('graph', {maxiiterations: 1337, writeProperty: 'p'})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key: maxiiterations (Did you mean [maxIterations]?)")
        );
    }

    @Test
    void dontSuggestExistingKeys() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.testProc.write('g', {writeProperty: 'p', writeConccurrency: 12})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key: writeConccurrency (Did you mean one of [writeConcurrency, writeToResultStore, concurrency]?)")
        );
    }

    @Test
    void misspelledRelationshipProjectionKeyWithSuggestion() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.graph.project('g', '*', {" +
                           "    FOO: {" +
                           "        tpe: 'FOO'" +
                           "    }" +
                           "})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key: tpe (Did you mean [type]?)")
        );
    }

    @Test
    void additionalRelationshipProjectionKey() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.graph.project('g', '*', {" +
                           "    FOO: {" +
                           "        type: 'FOO'," +
                           "        some: 'some'" +
                           "    }" +
                           "})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key: some")
        );
    }

    @Test
    void returnMultipleErrorsInConfigConstructionAtOnce() {
        loadCompleteGraph(DEFAULT_GRAPH_NAME);
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery(formatWithLocale("CALL gds.testProc.write('%s', {maxIterations: [1]})", DEFAULT_GRAPH_NAME))
        );

        String expectedMsg = "Multiple errors in configuration arguments:" + newLine +
                             "\t\t\t\tThe value of `maxIterations` must be of type `Integer` but was `ArrayList`." + newLine +
                             "\t\t\t\tNo value specified for the mandatory configuration parameter `writeProperty`";
        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, expectedMsg)
        );
    }

    @Test
    void misspelledNodeProjectionKeyWithSuggestion() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.graph.project('g', {" +
                           "    FOO: {" +
                           "        labl: 'Foo'" +
                           "    }" +
                           "}, '*')")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key: labl (Did you mean [label]?)")
        );
    }

    @Test
    void additionalNodeProjectionKey() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.graph.project('g', {" +
                           "    FOO: {" +
                           "        label: 'Foo'," +
                           "        some: 'some'" +
                           "    }" +
                           "}, '*')")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key: some")
        );
    }
}
