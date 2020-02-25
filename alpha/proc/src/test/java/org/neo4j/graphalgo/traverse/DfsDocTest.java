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
package org.neo4j.graphalgo.traverse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DfsDocTest extends BaseProcTest {
    private static final String DB_CYPHER =
        "CREATE" +
        "  (nA:Node {tag: 'a'})" +
        ", (nB:Node {tag: 'b'})" +
        ", (nC:Node {tag: 'c'})" +
        ", (nD:Node {tag: 'd'})" +
        ", (nE:Node {tag: 'e'})" +
        ", (nA)-[:REL {cost: 8.0}]->(nB)" +
        ", (nA)-[:REL {cost: 9.0}]->(nC)" +
        ", (nB)-[:REL {cost: 1.0}]->(nE)" +
        ", (nC)-[:REL {cost: 5.0}]->(nD)";

    private static final String CREATE_QUERY = "CALL gds.graph.create('myGraph', '*', '*', { relationshipProperties: 'cost' })";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(GraphCreateProc.class, TraverseProc.class);
        runQuery(DB_CYPHER);
        runQuery(CREATE_QUERY);
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void runDefault() {
        String result = runQuery(
            "MATCH (a:Node{tag:'a'})" +
            " WITH id(a) as startNode" +
            " CALL gds.alpha.dfs.stream('myGraph', {startNode: startNode})" +
            " YIELD path" +
            " UNWIND [ n in nodes(path) | n.tag ] as tags" +
            " RETURN tags" +
            " ORDER BY tags",
            Result::resultAsString
        );
        String expected = "+------+\n" +
                          "| tags |\n" +
                          "+------+\n" +
                          "| \"a\"  |\n" +
                          "| \"b\"  |\n" +
                          "| \"c\"  |\n" +
                          "| \"d\"  |\n" +
                          "| \"e\"  |\n" +
                          "+------+\n" +
                          "5 rows\n";
        assertEquals(expected, result);
    }

    // this could be indeterministic
    @Test
    void runWithTargetNodes() {
        String result = runQuery(
            "MATCH (a:Node{tag:'a'}), (d:Node{tag:'d'}), (e:Node{tag:'e'})" +
            " WITH id(a) as startNode, [id(d), id(e)] as targetNodes" +
            " CALL gds.alpha.dfs.stream('myGraph', {startNode: startNode, targetNodes: targetNodes})" +
            " YIELD path" +
            " UNWIND [ n in nodes(path) | n.tag ] as tags" +
            " RETURN tags" +
            " ORDER BY tags",
            Result::resultAsString
        );
        String expected = "+------+\n" +
                          "| tags |\n" +
                          "+------+\n" +
                          "| \"a\"  |\n" +
                          "| \"c\"  |\n" +
                          "| \"d\"  |\n" +
                          "+------+\n" +
                          "3 rows\n";
        assertEquals(expected, result);
    }

    @Test
    void runWithMaxDepth() {
        String result = runQuery(
            "MATCH (a:Node{tag:'a'})" +
            " WITH id(a) as startNode" +
            " CALL gds.alpha.dfs.stream('myGraph', {startNode: startNode, maxDepth: 1})" +
            " YIELD path" +
            " UNWIND [ n in nodes(path) | n.tag ] as tags" +
            " RETURN tags" +
            " ORDER BY tags",
            Result::resultAsString
        );
        String expected = "+------+\n" +
                          "| tags |\n" +
                          "+------+\n" +
                          "| \"a\"  |\n" +
                          "| \"b\"  |\n" +
                          "| \"c\"  |\n" +
                          "+------+\n" +
                          "3 rows\n";
        assertEquals(expected, result);
    }

    @Test
    void runWithMaxCost() {
        String result = runQuery(
            "MATCH (a:Node{tag:'a'})" +
            " WITH id(a) as startNode" +
            " CALL gds.alpha.dfs.stream('myGraph', {startNode: startNode, maxCost: 10, relationshipWeightProperty: 'cost'})" +
            " YIELD path" +
            " UNWIND [ n in nodes(path) | n.tag ] as tags" +
            " RETURN tags" +
            " ORDER BY tags",
            Result::resultAsString
        );
        String expected = "+------+\n" +
                          "| tags |\n" +
                          "+------+\n" +
                          "| \"a\"  |\n" +
                          "| \"b\"  |\n" +
                          "| \"c\"  |\n" +
                          "| \"e\"  |\n" +
                          "+------+\n" +
                          "4 rows\n";
        assertEquals(expected, result);
    }

}
