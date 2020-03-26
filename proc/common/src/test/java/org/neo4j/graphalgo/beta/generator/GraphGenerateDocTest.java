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
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class GraphGenerateDocTest extends BaseProcTest {

    private static final String NL = System.lineSeparator();

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(GraphGenerateProc.class);
    }

    @AfterEach
    void clearCommunities() {
        db.shutdown();
        GraphStoreCatalog.removeAllLoadedGraphs();
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
