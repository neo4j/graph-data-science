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

package org.neo4j.graphalgo.beta.generator;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class GraphGenerateDocTest extends BaseProcTest {

    private static final String NL = System.lineSeparator();

    public static final String DB_CYPHER =
        "CREATE" +
        "  (alice:User {name: 'Alice'})" +
        ", (bridget:User {name: 'Bridget'})" +
        ", (charles:User {name: 'Charles'})" +
        ", (doug:User {name: 'Doug'})" +
        ", (mark:User {name: 'Mark'})" +
        ", (michael:User {name: 'Michael'})" +
        ", (alice)-[:FOLLOWS {score: 1}]->(doug)" +
        ", (alice)-[:FOLLOWS {score: 2}]->(bridget)" +
        ", (alice)-[:FOLLOWS {score: 5}]->(charles)" +
        ", (mark)-[:FOLLOWS {score: 1.5}]->(doug)" +
        ", (mark)-[:FOLLOWS {score: 4.5}]->(michael)" +
        ", (bridget)-[:FOLLOWS {score: 1.5}]->(doug)" +
        ", (charles)-[:FOLLOWS {score: 2}]->(doug)" +
        ", (michael)-[:FOLLOWS {score: 1.5}]->(doug)";

    @BeforeEach
    void setupGraph() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );

        registerProcedures(GraphGenerateProc.class);
        registerFunctions(GetNodeFunc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void clearCommunities() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testGenerate() {
        @Language("Cypher")
        String query =
            "CALL gds.beta.graph.generate('myGraph', 42, toInteger(round(13.37)), {" +
            "   relationshipDistribution: 'POWER_LAW'" +
            "})" +
            " YIELD name, nodes, relationshipSeed, averageDegree, relationshipDistribution, relationshipProperty" +
            " RETURN name, nodes, relationshipSeed, averageDegree, relationshipDistribution, relationshipProperty";

        String expected =
            "+--------------------------------------------------------------------------------------------------------+" + NL +
            "| name      | nodes | relationshipSeed | averageDegree | relationshipDistribution | relationshipProperty |" + NL +
            "+--------------------------------------------------------------------------------------------------------+" + NL +
            "| \"myGraph\" | 42    | <null>           | 13.0          | \"POWER_LAW\"              | {}                   |" + NL +
            "+--------------------------------------------------------------------------------------------------------+" + NL +
            "1 row" + NL;

        String actual = runQuery(query, Result::resultAsString);
        assertEquals(expected, actual);
    }

}
