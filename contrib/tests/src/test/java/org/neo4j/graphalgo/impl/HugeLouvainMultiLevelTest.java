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
package org.neo4j.graphalgo.impl;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.impl.louvain.HugeLouvain;
import org.neo4j.graphdb.Label;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * (a)-(b)--(g)-(h)
 * \  /     \ /
 * (c)     (i)           (ABC)-(GHI)
 * \      /         =>    \   /
 * (d)-(e)                (DEF)
 * \  /
 * (f)
 *
 * @author mknblch
 */
public class HugeLouvainMultiLevelTest {

    private static final String COMPLEX_CYPHER =
            "CREATE (a:Node {name:'a'})\n" +
                    "CREATE (b:Node {name:'b'})\n" +
                    "CREATE (c:Node {name:'c'})\n" +
                    "CREATE (d:Node {name:'d'})\n" +
                    "CREATE (e:Node {name:'e'})\n" +
                    "CREATE (f:Node {name:'f'})\n" +
                    "CREATE (g:Node {name:'g'})\n" +
                    "CREATE (h:Node {name:'h'})\n" +
                    "CREATE (i:Node {name:'i'})\n" +
                    "CREATE" +

                    " (a)-[:TYPE]->(b),\n" +
                    " (a)-[:TYPE]->(c),\n" +
                    " (b)-[:TYPE]->(c),\n" +

                    " (g)-[:TYPE]->(h),\n" +
                    " (g)-[:TYPE]->(i),\n" +
                    " (h)-[:TYPE]->(i),\n" +

                    " (e)-[:TYPE]->(d),\n" +
                    " (e)-[:TYPE]->(f),\n" +
                    " (d)-[:TYPE]->(f),\n" +

                    " (a)-[:TYPE]->(g),\n" +
                    " (c)-[:TYPE]->(e),\n" +
                    " (f)-[:TYPE]->(i)";

    public static final Label LABEL = Label.label("Node");

    @Rule
    public ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private HugeGraph graph;

    private void setup(String cypher) {
        DB.execute(cypher);
        graph = (HugeGraph) new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withOptionalRelationshipWeightsFromProperty("w", 1.0)
                .asUndirected(true)
                .load(HugeGraphFactory.class);
    }

    @Test
    public void testComplex() {
        setup(COMPLEX_CYPHER);
        final HugeLouvain algorithm = new HugeLouvain(graph, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute(10, 10);
        final HugeLongArray[] dendogram = algorithm.getDendrogram();
        for (int i = 1; i <= dendogram.length; i++) {
            if (null == dendogram[i - 1]) {
                break;
            }
            System.out.println("level " + i + ": " + dendogram[i - 1]);
        }

        assertArrayEquals(new long[]{0, 0, 0, 1, 1, 1, 2, 2, 2}, dendogram[0].toArray());
        assertArrayEquals(new long[]{0, 0, 0, 1, 1, 1, 2, 2, 2}, algorithm.getCommunityIds().toArray());
        assertEquals(0.53, algorithm.getFinalModularity(), 0.01);
        assertArrayEquals(new double[]{0.53}, algorithm.getModularities(), 0.01);
    }

    @Test
    public void testComplexRNL() {
        setup(COMPLEX_CYPHER);
        final HugeLouvain algorithm = new HugeLouvain(graph, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute(10, 10, true);
        final HugeLongArray[] dendogram = algorithm.getDendrogram();
        for (int i = 1; i <= dendogram.length; i++) {
            if (null == dendogram[i - 1]) {
                break;
            }
            System.out.println("level " + i + ": " + dendogram[i - 1]);
        }

    }
}
