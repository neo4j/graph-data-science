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
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * (a)-(b)---(e)-(f)
 * | X |     | X |   (z)
 * (c)-(d)   (g)-(h)
 */
class LouvainWeightedGraphTest extends LouvainTestBase {

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
            ", (z:Node {name: 'z'})" +

            ", (a)-[:TYPE]->(b)" +
            ", (a)-[:TYPE]->(c)" +
            ", (a)-[:TYPE]->(d)" +
            ", (c)-[:TYPE]->(d)" +
            ", (c)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(d)" +

            ", (e)-[:TYPE]->(f)" +
            ", (e)-[:TYPE]->(g)" +
            ", (e)-[:TYPE]->(h)" +
            ", (f)-[:TYPE]->(h)" +
            ", (f)-[:TYPE]->(g)" +
            ", (g)-[:TYPE]->(h)" +

            ", (e)-[:TYPE {weight: 4}]->(b)";

    private static final int MAX_ITERATIONS = 10;
    private static final Label LABEL = Label.label("Node");
    private static final String ABCDEFGHZ = "abcdefghz";

    @Override
    void setupGraphDb(Graph graph) {
        try (Transaction transaction = DB.beginTx()) {
            for (int i = 0; i < ABCDEFGHZ.length(); i++) {
                final String value = String.valueOf(ABCDEFGHZ.charAt(i));
                final long id = graph.toMappedNodeId(DB.findNode(LABEL, "name", value).getId());
                nameMap.put(value, (int) id);
            }
            transaction.success();
        }
    }

    void printCommunities(Louvain louvain) {
        try (Transaction ignored = DB.beginTx()) {
            louvain
                    .resultStream()
                    .forEach(r -> System.out.println(DB.getNodeById(r.nodeId).getProperty("name") + ":" + r.community));
        }
    }

    @AllGraphTypesTest
    void testWeightedLouvain(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);
        final Louvain louvain =
                new Louvain(graph, DEFAULT_CONFIG, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                        .withProgressLogger(TestProgressLogger.INSTANCE)
                        .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                        .compute();

        final HugeLongArray[] dendogram = louvain.getDendrogram();
        for (int i = 0; i < dendogram.length; i++) {
            if (null == dendogram[i]) {
                break;
            }
            System.out.println("level " + i + ": " + dendogram[i]);
        }
        printCommunities(louvain);
        System.out.println("louvain.getRuns() = " + louvain.getLevel());
        System.out.println("louvain.communityCount() = " + louvain.communityCount());
        assertCommunities(louvain);
        assertTrue(louvain.getLevel() < MAX_ITERATIONS, "Maximum iterations > " + MAX_ITERATIONS);
    }

    @AllGraphTypesTest
    void testWeightedRandomNeighborLouvain(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);
        final Louvain louvain =
                new Louvain(graph, DEFAULT_CONFIG, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                        .withProgressLogger(TestProgressLogger.INSTANCE)
                        .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                        .compute();

        final HugeLongArray[] dendogram = louvain.getDendrogram();
        for (int i = 0; i < dendogram.length; i++) {
            if (null == dendogram[i]) {
                break;
            }
            System.out.println("level " + i + ": " + dendogram[i]);
        }
        printCommunities(louvain);
        System.out.println("louvain.getRuns() = " + louvain.getLevel());
        System.out.println("louvain.communityCount() = " + louvain.communityCount());
    }

    void assertCommunities(Louvain louvain) {
        assertUnion(new String[]{"a", "c", "d"}, louvain.getCommunityIds());
        assertUnion(new String[]{"f", "g", "h"}, louvain.getCommunityIds());
        assertDisjoint(new String[]{"a", "f", "z"}, louvain.getCommunityIds());
        assertUnion(new String[]{"b", "e"}, louvain.getCommunityIds());
    }

}
