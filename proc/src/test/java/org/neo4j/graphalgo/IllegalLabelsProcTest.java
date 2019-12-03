/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import org.neo4j.graphalgo.unionfind.UnionFindProc;
import org.neo4j.graphdb.QueryExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.core.utils.ExceptionUtil.rootCause;

class IllegalLabelsProcTest extends ProcTestBase {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:A {id: 0}) " +
            ", (b:B {id: 1}) " +
            ", (a)-[:X]->(b)";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(UnionFindProc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
    }

    @AllGraphNamesTest
    void testUnionFindStreamWithInvalidNodeLabel(String graphName) {
        QueryExecutionException ex = assertThrows(
                QueryExecutionException.class,
                () -> runQuery(String.format("CALL algo.unionFind.stream('C', '',{graph:'%s'})", graphName)));
        assertEquals(IllegalArgumentException.class, rootCause(ex).getClass());
        assertThat(ex.getMessage(), containsString("Invalid node projection, one or more labels not found: 'C'"));
    }

    @AllGraphNamesTest
    void testUnionFindStreamWithInvalidRelType(String graphName) {
        QueryExecutionException ex = assertThrows(QueryExecutionException.class, () ->
                runQuery(String.format("CALL algo.unionFind.stream('', 'Y',{graph:'%s'})", graphName)));
        assertEquals(IllegalArgumentException.class, rootCause(ex).getClass());
        assertThat(ex.getMessage(), containsString("Relationship type(s) not found: 'Y'"));
    }

    @AllGraphNamesTest
    void testUnionFindStreamWithValidNodeLabelAndInvalidRelType(String graphName) {
        QueryExecutionException ex = assertThrows(QueryExecutionException.class, () ->
                runQuery(String.format("CALL algo.unionFind.stream('A', 'Y',{graph:'%s'})", graphName)));
        assertEquals(IllegalArgumentException.class, rootCause(ex).getClass());
        assertThat(ex.getMessage(), containsString("Relationship type(s) not found: 'Y'"));
    }

    @AllGraphNamesTest
    void testUnionFindStreamWithMultipleInvaludRelTypes(String graphName) {
        QueryExecutionException ex = assertThrows(QueryExecutionException.class, () ->
                runQuery(String.format("CALL algo.unionFind.stream('A', 'Y | Z',{graph:'%s'})", graphName)));
        assertEquals(IllegalArgumentException.class, rootCause(ex).getClass());
        assertThat(ex.getMessage(), containsString("Relationship type(s) not found: 'Y', 'Z'"));
    }
}
