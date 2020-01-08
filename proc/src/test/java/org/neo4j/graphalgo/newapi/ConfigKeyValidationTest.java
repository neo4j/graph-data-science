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
package org.neo4j.graphalgo.newapi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.pagerank.PageRankStreamProc;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.graphalgo.ThrowableRootCauseMatcher.rootCause;

class ConfigKeyValidationTest extends BaseProcTest {

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(GraphCreateProc.class, PageRankStreamProc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void additionalKeyForGraphCreate() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.graph.create('foo', '*', '*', {readConcurrency: 42, maxIterations: 1337})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key(s): [maxIterations]")
        );
    }

    @Test
    void additionalKeyForExplicitLoading() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.algo.pageRank.stream('foo', {maxIterations: 1337, nodeProjection: '*'})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key(s): [nodeProjection]")
        );
    }

    @Test
    void additionalKeyForImplicitLoading() {
        QueryExecutionException exception = Assertions.assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.algo.pageRank.stream({maxIterations: 1337, nodeProjection: '*', some: 'key'})")
        );

        assertThat(
            exception,
            rootCause(IllegalArgumentException.class, "Unexpected configuration key(s): [some]")
        );
    }
}
