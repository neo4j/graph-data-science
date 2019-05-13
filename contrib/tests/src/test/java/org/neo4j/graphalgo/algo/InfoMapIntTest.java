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
package org.neo4j.graphalgo.algo;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.InfoMapProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.proc.PageRankProc;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.BitSet;

import static org.junit.Assert.assertEquals;

/**
 *
 * TODO REPAIR
 *
 * Graph:
 *
 *        (b)        (e)
 *       /  \       /  \     (x)
 *     (a)--(c)---(d)--(f)
 *
 * @author mknblch
 */
public class InfoMapIntTest {


    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'} )\n" +
                "CREATE (b:Node {name:'b'} )\n" +
                "CREATE (c:Node {name:'c'} )\n" +
                "CREATE (d:Node {name:'d'} )\n" +
                "CREATE (e:Node {name:'e'} )\n" +
                "CREATE (f:Node {name:'f'} )\n" +
                "CREATE (x:Node {name:'x'} )\n" +
                        "CREATE" +
                        " (b)-[:TYPE {v:1.0}]->(a),\n" +
                        " (a)-[:TYPE {v:1.0}]->(c),\n" +
                        " (c)-[:TYPE {v:1.0}]->(a),\n" +
                        " (d)-[:TYPE {v:2.0}]->(c),\n" +
                        " (d)-[:TYPE {v:1.0}]->(e),\n" +
                        " (d)-[:TYPE {v:1.0}]->(f),\n" +
                        " (e)-[:TYPE {v:1.0}]->(f)";

        db.execute(cypher);
        db.resolveDependency(Procedures.class).registerProcedure(InfoMapProc.class);
        db.resolveDependency(Procedures.class).registerProcedure(PageRankProc.class);
    }

    @Test
    public void testUnweighted() throws Exception {

        final BitSet bitSet = new BitSet(8);

        db.execute("CALL algo.infoMap('Node', 'TYPE', {iterations:15, writeProperty:'c'})").close();

        db.execute("MATCH (n) RETURN n").accept(row -> {
            final Node node = row.getNode("n");
            bitSet.set((Integer) node.getProperty("c"));
            return true;
        });

        assertEquals(3, bitSet.cardinality());
    }


    @Test
    public void testUnweightedStream() throws Exception {

        final BitSet bitSet = new BitSet(8);

        db.execute("CALL algo.infoMap.stream('Node', 'TYPE', {iterations:15}) YIELD nodeId, community")
                .accept(row -> {
                    bitSet.set((Integer) row.getNumber("community").intValue());
                    return true;
                });

        assertEquals(3, bitSet.cardinality());

    }


    @Test
    public void testWeighted() throws Exception {

        final BitSet bitSet = new BitSet(8);

        db.execute("CALL algo.infoMap('Node', 'TYPE', {weightProperty:'v', writeProperty:'c'})").close();

        db.execute("MATCH (n) RETURN n").accept(row -> {
            final Node node = row.getNode("n");
            bitSet.set((Integer) node.getProperty("c"));
            return true;
        });

        assertEquals(2, bitSet.cardinality());

    }


    @Test
    public void testWeightedStream() throws Exception {

        final BitSet bitSet = new BitSet(8);

        db.execute("CALL algo.infoMap.stream('Node', 'TYPE', {iterations:15, weightProperty:'v'}) YIELD nodeId, community")
                .accept(row -> {
                    bitSet.set((Integer) row.getNumber("community").intValue());
                    return true;
                });

        assertEquals(2, bitSet.cardinality());

    }

    @Test
    public void testPredefinedPageRankStream() throws Exception {


        db.execute("CALL algo.pageRank('Node', 'TYPE', {writeProperty:'p', iterations:1}) YIELD nodes").close();

        final BitSet bitSet = new BitSet(8);

        db.execute("CALL algo.infoMap.stream('Node', 'TYPE', {pageRankProperty:'p'}) YIELD nodeId, community")
                .accept(row -> {
                    bitSet.set((Integer) row.getNumber("community").intValue());
                    return true;
                });

        assertEquals(3, bitSet.cardinality());

    }

}
