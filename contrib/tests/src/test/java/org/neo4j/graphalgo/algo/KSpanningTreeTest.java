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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphalgo.KSpanningTreeProc;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author mknblch
 */
public class KSpanningTreeTest {

    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +

                        "CREATE" +
                        " (a)-[:TYPE {w:3.0}]->(b),\n" +
                        " (a)-[:TYPE {w:2.0}]->(c),\n" +
                        " (a)-[:TYPE {w:1.0}]->(d),\n" +
                        " (b)-[:TYPE {w:1.0}]->(c),\n" +
                        " (d)-[:TYPE {w:3.0}]->(c)";

        DB.resolveDependency(Procedures.class).registerProcedure(KSpanningTreeProc.class);
        DB.execute(cypher);
    }

    @Rule
    public ExpectedException exceptions = ExpectedException.none();

    @Test
    public void testMax() {
        final String cypher = "MATCH (n:Node {name:'a'}) WITH n CALL algo.spanningTree.kmax(null, null, 'w', id(n), 2, {graph:'huge'}) " +
                "YIELD loadMillis, computeMillis, writeMillis RETURN loadMillis, computeMillis, writeMillis";
        DB.execute(cypher).accept(row -> {

            assertTrue(row.getNumber("loadMillis").longValue() >= 0);
            assertTrue(row.getNumber("writeMillis").longValue() >= 0);
            assertTrue(row.getNumber("computeMillis").longValue() >= 0);
            return true;
        });

        final HashMap<String, Integer> communities = new HashMap<>();

        DB.execute("MATCH (n) WHERE exists(n.partition) RETURN n.name as name, n.partition as p").accept(row -> {
            final String name = row.getString("name");
            final int p = row.getNumber("p").intValue();
            communities.put(name, p);
            return true;
        });

        assertEquals(communities.get("a"), communities.get("b"));
        assertEquals(communities.get("d"), communities.get("c"));
        assertNotEquals(communities.get("a"), communities.get("c"));
    }
    @Test
    public void testMin() {
        final String cypher = "MATCH (n:Node {name:'a'}) WITH n CALL algo.spanningTree.kmin(null, null, 'w', id(n), 2, {graph:'huge'}) " +
                "YIELD loadMillis, computeMillis, writeMillis RETURN loadMillis, computeMillis, writeMillis";
        DB.execute(cypher).accept(row -> {

            assertTrue(row.getNumber("loadMillis").longValue() >= 0);
            assertTrue(row.getNumber("writeMillis").longValue() >= 0);
            assertTrue(row.getNumber("computeMillis").longValue() >= 0);
            return true;
        });

        final HashMap<String, Integer> communities = new HashMap<>();

        DB.execute("MATCH (n) WHERE exists(n.partition) RETURN n.name as name, n.partition as p").accept(row -> {
            final String name = row.getString("name");
            final int p = row.getNumber("p").intValue();
            communities.put(name, p);
            return true;
        });

        assertEquals(communities.get("a"), communities.get("d"));
        assertEquals(communities.get("b"), communities.get("c"));
        assertNotEquals(communities.get("a"), communities.get("b"));
    }

}
