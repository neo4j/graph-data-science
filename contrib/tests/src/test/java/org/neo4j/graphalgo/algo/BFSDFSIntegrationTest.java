/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.algo;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.graphalgo.TraverseProc;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.*;

/**
 * Graph:
 *
 *     (b)   (e)
 *   2/ 1\ 2/ 1\
 * >(a)  (d)  ((g))
 *   1\ 2/ 1\ 2/
 *    (c)   (f)
 *
 * @author mknblch
 */
public class BFSDFSIntegrationTest {


    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {


        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE {cost:2.0}]->(b),\n" +
                        " (a)-[:TYPE {cost:1.0}]->(c),\n" +
                        " (b)-[:TYPE {cost:1.0}]->(d),\n" +
                        " (c)-[:TYPE {cost:2.0}]->(d),\n" +
                        " (d)-[:TYPE {cost:1.0}]->(e),\n" +
                        " (d)-[:TYPE {cost:2.0}]->(f),\n" +
                        " (e)-[:TYPE {cost:2.0}]->(g),\n" +
                        " (f)-[:TYPE {cost:1.0}]->(g)";

        db.resolveDependency(Procedures.class).registerProcedure(TraverseProc.class);
        db.execute(cypher);
    }

    @Rule
    public ExpectedException exceptions = ExpectedException.none();

    private static long id(String name) {
        final Node[] node = new Node[1];
        db.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return node[0].getId();
    }

    private static String name(long id) {
        final String[] node = new String[1];
        db.execute("MATCH (n:Node) WHERE id(n) = " + id + " RETURN n.name as name").accept(row -> {
            node[0] = row.getString("name");
            return false;
        });
        return node[0];
    }

    /**
     * test if all both arrays contain the same nodes. not necessarily in
     * same order
     */
    static void assertContains(String[] expected, List<Long> nodeIds) {
        assertEquals("expected " + Arrays.toString(expected) + " | given [" + nodeIds+ "]", expected.length, nodeIds.size());
        for (String ex : expected) {
            final long id = id(ex);
            if (!nodeIds.contains(id)) {
                fail(ex + "(" + id + ") not in " + nodeIds);
            }
        }
    }

    @Test
    public void testFindAnyOf() {
        final String cypher = "MATCH (n:Node {name:'a'}) WITH id(n) as s CALL algo.dfs.stream('Node', 'Type', '>', s, {targetNodes:[4,5]}) YIELD nodeIds RETURN nodeIds";
        db.execute(cypher).accept(row -> {
            List<Long> nodeIds = (List<Long>) row.get("nodeIds");
            assertEquals(4, nodeIds.size());
            return true;
        });
    }

    @Test
    public void testMaxDepthOut() {
        final String cypher = "MATCH (n:Node {name:'a'}) WITH id(n) as s CALL algo.dfs.stream('Node', 'Type', '>', s, {maxDepth:2}) YIELD nodeIds RETURN nodeIds";
        db.execute(cypher).accept(row -> {
            List<Long> nodeIds = (List<Long>) row.get("nodeIds");
            assertContains(new String[]{"a", "b", "c", "d"}, nodeIds);
            return true;
        });
    }

    @Test
    public void testMaxDepthIn() {
        final String cypher = "MATCH (n:Node {name:'g'}) WITH id(n) as s CALL algo.dfs.stream('Node', 'Type', '<', s, {maxDepth:2}) YIELD nodeIds RETURN nodeIds";
        db.execute(cypher).accept(row -> {
            List<Long> nodeIds = (List<Long>) row.get("nodeIds");
            assertContains(new String[]{"g", "e", "f", "d"}, nodeIds);
            return true;
        });
    }

}
