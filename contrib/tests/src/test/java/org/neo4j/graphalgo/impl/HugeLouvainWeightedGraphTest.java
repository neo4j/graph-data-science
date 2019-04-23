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
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
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
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * (a)-(b)---(e)-(f)
 *  | X |     | X |   (z)
 * (c)-(d)   (g)-(h)
 *
 *  @author mknblch
 */
public class HugeLouvainWeightedGraphTest {

    private static final String unidirectional =
            "CREATE (a:Node {name:'a'})\n" +
                    "CREATE (b:Node {name:'b'})\n" +
                    "CREATE (c:Node {name:'c'})\n" +
                    "CREATE (d:Node {name:'d'})\n" +
                    "CREATE (e:Node {name:'e'})\n" +
                    "CREATE (f:Node {name:'f'})\n" +
                    "CREATE (g:Node {name:'g'})\n" +
                    "CREATE (h:Node {name:'h'})\n" +
                    "CREATE (z:Node {name:'z'})\n" +
                    "CREATE" +
                    " (a)-[:TYPE]->(b),\n" +
                    " (a)-[:TYPE]->(c),\n" +
                    " (a)-[:TYPE]->(d),\n" +
                    " (c)-[:TYPE]->(d),\n" +
                    " (c)-[:TYPE]->(b),\n" +
                    " (b)-[:TYPE]->(d),\n" +

                    " (e)-[:TYPE]->(f),\n" +
                    " (e)-[:TYPE]->(g),\n" +
                    " (e)-[:TYPE]->(h),\n" +
                    " (f)-[:TYPE]->(h),\n" +
                    " (f)-[:TYPE]->(g),\n" +
                    " (g)-[:TYPE]->(h),\n" +

                    " (e)-[:TYPE {w:4}]->(b)";


    private static final int MAX_ITERATIONS = 10;
    public static final Label LABEL = Label.label("Node");
    public static final String ABCDEFGHZ = "abcdefghz";

    @Rule
    public ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private HugeGraph graph;
    private final Map<String, Integer> nameMap;

    public HugeLouvainWeightedGraphTest() {
        nameMap = new HashMap<>();
    }

    private void setup(String cypher) {
        DB.execute(cypher);
        graph = (HugeGraph) new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withOptionalRelationshipWeightsFromProperty("w", 1.0)
                .asUndirected(true)
                .load(HugeGraphFactory.class);

        try (Transaction transaction = DB.beginTx()) {
            for (int i = 0; i < ABCDEFGHZ.length(); i++) {
                final String value = String.valueOf(ABCDEFGHZ.charAt(i));
                final int id = graph.toMappedNodeId(DB.findNode(LABEL, "name", value).getId());
                nameMap.put(value, id);
            }
            transaction.success();
        }
    }

    public void printCommunities(HugeLouvain louvain) {
        DB.executeAndCommit(db -> {
            louvain.resultStream().forEach(r -> {
                System.out.println(db.getNodeById(r.nodeId).getProperty("name") + ":" + r.community);
            });
        });
    }

    @Test
    public void testWeightedLouvain() throws Exception {
        setup(unidirectional);
        final HugeLouvain louvain =
                new HugeLouvain(graph,Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                        .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute(10, 10);

        final HugeLongArray[] dendogram = louvain.getDendrogram();
        for (int i = 0; i < dendogram.length; i++) {
            if (null == dendogram[i]) {
                break;
            }
            System.out.println("level " + i + ": " + dendogram[i]);
        }
        printCommunities(louvain);
        System.out.println("louvain.getRuns() = " + louvain.getLevel());
        System.out.println("louvain.getCommunityCount() = " + louvain.getCommunityCount());
        assertCommunities(louvain);
        assertTrue("Maximum iterations > " + MAX_ITERATIONS,louvain.getLevel() < MAX_ITERATIONS);
    }

    @Test
    public void testWeightedRandomNeighborLouvain() throws Exception {
        setup(unidirectional);
        final HugeLouvain louvain =
                new HugeLouvain(graph,Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                        .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute(10, 10, true);

        final HugeLongArray[] dendogram = louvain.getDendrogram();
        for (int i = 0; i < dendogram.length; i++) {
            if (null == dendogram[i]) {
                break;
            }
            System.out.println("level " + i + ": " + dendogram[i]);
        }
        printCommunities(louvain);
        System.out.println("louvain.getRuns() = " + louvain.getLevel());
        System.out.println("louvain.getCommunityCount() = " + louvain.getCommunityCount());
    }

    public void assertCommunities(HugeLouvain louvain) {
        assertUnion(new String[]{"a", "c", "d"}, louvain.getCommunityIds());
        assertUnion(new String[]{"f", "g", "h"}, louvain.getCommunityIds());
        assertDisjoint(new String[]{"a", "f", "z"}, louvain.getCommunityIds());
        assertUnion(new String[]{"b", "e"}, louvain.getCommunityIds());
    }

    private void assertUnion(String[] nodeNames, HugeLongArray values) {
        final long[] communityIds = values.toArray();
        long current = -1L;
        for (String name : nodeNames) {
            if (!nameMap.containsKey(name)) {
                throw new IllegalArgumentException("unknown node name: " + name);
            }
            final int id = nameMap.get(name);
            if (current == -1L) {
                current = communityIds[id];
            } else {
                assertEquals("Node " + name + " belongs to wrong community " + communityIds[id], current, communityIds[id]);
            }
        }
    }

    private void assertDisjoint(String[] nodeNames, HugeLongArray values) {
        final long[] communityIds = values.toArray();
        final LongSet set = new LongHashSet();
        for (String name : nodeNames) {
            final long communityId = communityIds[nameMap.get(name)];
            assertTrue("Node " + name + " belongs to wrong community " + communityId, set.add(communityId));
        }
    }
}
