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

import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * (a)-(b)--(g)-(h)
 * \  /     \ /
 * (c)     (i)           (ABC)-(GHI)
 * \      /         =>    \   /
 * (d)-(e)                (DEF)
 * \  /
 * (f)
 */
class LouvainMultiLevelTest extends LouvainTestBase {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (d:Node {name: 'd'})" +
            ", (e:Node {name: 'e'})" +
            ", (f:Node {name: 'f'})" +
            ", (g:Node {name: 'g'})" +
            ", (h:Node {name: 'h'})" +
            ", (i:Node {name: 'i'})" +

            ", (a)-[:TYPE {weight: 1.0}]->(b)" +
            ", (a)-[:TYPE {weight: 1.0}]->(c)" +
            ", (b)-[:TYPE {weight: 1.0}]->(c)" +

            ", (g)-[:TYPE {weight: 1.0}]->(h)" +
            ", (g)-[:TYPE {weight: 1.0}]->(i)" +
            ", (h)-[:TYPE {weight: 1.0}]->(i)" +

            ", (e)-[:TYPE {weight: 1.0}]->(d)" +
            ", (e)-[:TYPE {weight: 1.0}]->(f)" +
            ", (d)-[:TYPE {weight: 1.0}]->(f)" +

            ", (a)-[:TYPE {weight: 1.0}]->(g)" +
            ", (c)-[:TYPE {weight: 1.0}]->(e)" +
            ", (f)-[:TYPE {weight: 1.0}]->(i)";

    @Override
    void setupGraphDb(Graph graph) {
    }

    @AllGraphTypesTest
    void testComplex(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);
        final Louvain algorithm = new Louvain(graph, DEFAULT_CONFIG, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute();
        final HugeLongArray[] dendogram = algorithm.getDendrogram();
        for (int i = 1; i <= dendogram.length; i++) {
            if (null == dendogram[i - 1]) {
                break;
            }
        }

        assertArrayEquals(new long[]{0, 0, 0, 1, 1, 1, 2, 2, 2}, dendogram[0].toArray());
        assertArrayEquals(new long[]{0, 0, 0, 1, 1, 1, 2, 2, 2}, algorithm.getCommunityIds().toArray());
        assertEquals(0.53, algorithm.getFinalModularity(), 0.01);
        assertArrayEquals(new double[]{0.53}, algorithm.getModularities(), 0.01);
    }

    @AllGraphTypesTest
    void testComplexRNL(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);
        final Louvain algorithm = new Louvain(graph, DEFAULT_CONFIG, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute(10, 10, true);
        final HugeLongArray[] dendogram = algorithm.getDendrogram();
        for (int i = 1; i <= dendogram.length; i++) {
            if (null == dendogram[i - 1]) {
                break;
            }
        }

    }
}
