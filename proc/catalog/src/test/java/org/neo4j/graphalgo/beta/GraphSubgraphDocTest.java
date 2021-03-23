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
package org.neo4j.graphalgo.beta;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.extension.Neo4jGraph;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class GraphSubgraphDocTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB =
        "CREATE" +
        "  (a:Person { age: 16 })" +
        ", (b:Person { age: 18 })" +
        ", (c:Person { age: 20 })" +
        ", (a)-[:KNOWS { since: 2010 }]->(b)" +
        ", (a)-[:KNOWS { since: 2018 }]->(c)";

    private static final String NL = System.lineSeparator();

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(GraphCreateProc.class, GraphSubgraphProc.class);

        runQuery(GdsCypher.call()
            .withNodeLabel("Person")
            .withNodeProperty("age")
            .withRelationshipType("KNOWS")
            .withRelationshipProperty("since")
            .graphCreate("social-graph")
            .yields()
        );
    }

    @AfterEach
    void cleanup() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testExample1() {
        @Language("Cypher")
        String query =
            "CALL gds.beta.graph.subgraph('social-graph', 'teenagers', {" +
            "  nodeFilter: 'n.age > 13 AND n.age <= 18'" +
            "}) YIELD graphName, subgraphName, nodeCount, relationshipCount";

        String expected =
            "+---------------------------------------------------------------+" + NL +
            "| graphName      | subgraphName | nodeCount | relationshipCount |" + NL +
            "+---------------------------------------------------------------+" + NL +
            "| \"social-graph\" | \"teenagers\"  | 2         | 1                 |" + NL +
            "+---------------------------------------------------------------+" + NL +
            "1 row" + NL;

        String actual = runQuery(query, Result::resultAsString);
        assertEquals(expected, actual);
    }

    @Test
    void testExample2() {
        @Language("Cypher")
        String query =
            "CALL gds.beta.graph.subgraph('social-graph', 'teenagers', {" +
            "  nodeFilter: 'n.age > 13 AND n.age <= 18'," +
            "  relationshipFilter: 'r.since >= 2012'" +
            "}) YIELD graphName, subgraphName, nodeCount, relationshipCount";

        String expected =
            "+---------------------------------------------------------------+" + NL +
            "| graphName      | subgraphName | nodeCount | relationshipCount |" + NL +
            "+---------------------------------------------------------------+" + NL +
            "| \"social-graph\" | \"teenagers\"  | 2         | 0                 |" + NL +
            "+---------------------------------------------------------------+" + NL +
            "1 row" + NL;

        String actual = runQuery(query, Result::resultAsString);
        assertEquals(expected, actual);
    }


}
