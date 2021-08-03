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
import org.neo4j.gds.test.TestProc;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphdb.QueryExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.graphalgo.ThrowableRootCauseMatcher.rootCause;

class ConfigKeyValidationTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class, TestProc.class);
    }

    @Test
    void additionalKeyForGraphCreate() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.graph.create('foo', '*', '*', {readConcurrency: 4, maxIterations: 1337})")
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
            () -> runQuery("CALL gds.testProc.test('foo', {nodeProjection: '*', writeProperty: 'p'})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key: nodeProjection")
        );
    }

    @Test
    void additionalKeyForImplicitLoading() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.testProc.test({nodeProjection: '*', relationshipProjection: '*', writeProperty: 'p', some: 'key'})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key: some")
        );
    }

    @Test
    void misspelledProjectionKeyWithSuggestion() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.testProc.test({nodeProjections: '*', relationshipProjection: '*'})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Missing information for implicit graph creation. No value specified for the mandatory configuration parameter `nodeProjection` (a similar parameter exists: [nodeProjections])")
        );
    }

    @Test
    void misspelledRequiredKeyWithSuggestion() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.testProc.test({wirteProperty: 'foo', nodeProjection: '*', relationshipProjection: '*'})")
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
            () -> runQuery("CALL gds.testProc.test({maxiiterations: 1337, writeProperty: 'p', nodeProjection: '*', relationshipProjection: '*'})")
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
            () -> runQuery("CALL gds.testProc.test({writeProperty: 'p', writeConccurrency: 12,nodeProjection: '*', relationshipProjection: '*'})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key: writeConccurrency (Did you mean one of [writeConcurrency, readConcurrency, concurrency]?)")
        );
    }

    @Test
    void misspelledRelationshipProjectionKeyWithSuggestion() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.testProc.test({ nodeProjection: '*', relationshipProjection: {" +
                           "    FOO: {" +
                           "        tpe: 'FOO'" +
                           "    }" +
                           "}, writeProperty: 'p'})")
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
            () -> runQuery("CALL gds.testProc.test({ nodeProjection: '*', relationshipProjection: {" +
                           "    FOO: {" +
                           "        type: 'FOO'," +
                           "        some: 'some'" +
                           "    }" +
                           "}, writeProperty: 'p'})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key: some")
        );
    }

    @Test
    void returnMultipleErrorsInConfigConstructionAtOnce() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.testProc.test({nodeProjection: '*', relationshipProjection: '*', maxIterations: [1]})")
        );

        String expectedMsg = "Multiple errors in configuration arguments:\n" +
                             "\t\t\t\tThe value of `maxIterations` must be of type `Integer` but was `ArrayList`.\n" +
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
            () -> runQuery("CALL gds.testProc.test({ nodeProjection: {" +
                           "    FOO: {" +
                           "        labl: 'Foo'" +
                           "    }" +
                           "}, relationshipProjection: '*' })")
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
            () -> runQuery("CALL gds.testProc.test({ nodeProjection: {" +
                           "    FOO: {" +
                           "        label: 'Foo'," +
                           "        some: 'some'" +
                           "    }" +
                           "}, relationshipProjection: '*' })")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key: some")
        );
    }
}
