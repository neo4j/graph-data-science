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

// TODO port to new API, Bring back Louvain and PageRank
public class WritingProcTest extends ProcTestBase {

//    @BeforeEach
//    void setup() throws Exception {
//        db = TestDatabaseCreator.createTestDatabase();
//        registerProcedures(
//        );
//    }
//
//    @AfterEach
//    void teardown() {
//        db.shutdown();
//    }
//
//    @ParameterizedTest
//    @MethodSource("parameters")
//    void shouldFailToWriteInCypherLoaderQueries(String proc, String nodeQuery, String relQuery) {
//        String query = String.format(
//                "CALL %s(" +
//                "  '%s'," +
//                "  '%s'," +
//                "  {" +
//                "    graph: 'cypher'" +
//                "  })",
//                proc, nodeQuery, relQuery);
//        QueryExecutionException ex = assertThrows(QueryExecutionException.class, () -> runQuery(query).hasNext());
//        Throwable root = ExceptionUtil.rootCause(ex);
//        assertTrue(root instanceof IllegalArgumentException);
//        assertThat(root.getMessage(), containsString("Query must be read only. Query: "));
//    }
//
//    private static Stream<Arguments> parameters() {
//        return TestSupport.crossArguments(toArguments(WritingProcTest::procsToTest), () -> Stream.of(
//                Arguments.of("CREATE (n) RETURN id(n) AS id", "RETURN 0 AS source, 1 AS target"),
//                Arguments.of("RETURN 0 AS id", "CREATE (n)-[:REL]->(m) RETURN id(n) AS source, id(m) AS target")
//        ));
//    }
//
//    private static Stream<String> procsToTest() {
//        return Stream.of(
//        );
//    }
}
