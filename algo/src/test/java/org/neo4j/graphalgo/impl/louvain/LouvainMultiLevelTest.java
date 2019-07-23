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
package org.neo4j.graphalgo.impl.louvain;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.neo4j.graphalgo.HeavyHugeTester;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphalgo.impl.louvain.LouvainTest.DEFAULT_CONFIG;

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
public class LouvainMultiLevelTest extends HeavyHugeTester {

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

                    " (a)-[:TYPE {weight: 1.0}]->(b),\n" +
                    " (a)-[:TYPE {weight: 1.0}]->(c),\n" +
                    " (b)-[:TYPE {weight: 1.0}]->(c),\n" +

                    " (g)-[:TYPE {weight: 1.0}]->(h),\n" +
                    " (g)-[:TYPE {weight: 1.0}]->(i),\n" +
                    " (h)-[:TYPE {weight: 1.0}]->(i),\n" +

                    " (e)-[:TYPE {weight: 1.0}]->(d),\n" +
                    " (e)-[:TYPE {weight: 1.0}]->(f),\n" +
                    " (d)-[:TYPE {weight: 1.0}]->(f),\n" +

                    " (a)-[:TYPE {weight: 1.0}]->(g),\n" +
                    " (c)-[:TYPE {weight: 1.0}]->(e),\n" +
                    " (f)-[:TYPE {weight: 1.0}]->(i)";

    @Rule
    public ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private Class<? extends GraphFactory> graphImpl;
    private Graph graph;

    public LouvainMultiLevelTest(Class<? extends GraphFactory> graphImpl, String name) {
        super(graphImpl);
        this.graphImpl = graphImpl;
    }

    private void setup(String cypher) {
        DB.execute(cypher);
        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withOptionalRelationshipWeightsFromProperty("weight", 1.0)
                .asUndirected(true)
                .load(graphImpl);
    }

    @Test
    public void testComplex() {
        setup(COMPLEX_CYPHER);
        final Louvain algorithm = new Louvain(graph, DEFAULT_CONFIG, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute();
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
        final Louvain algorithm = new Louvain(graph, DEFAULT_CONFIG, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
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
