/**
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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * (a)-(b)-(d)
 * | /            -> (abc)-(d)
 * (c)
 *
 * @author mknblch
 */
public class HugeLouvainTest1 {

    private static final String unidirectional =
            "CREATE (a:Node {name:'a'})\n" +
                    "CREATE (b:Node {name:'b'})\n" +
                    "CREATE (c:Node {name:'c'})\n" +
                    "CREATE (d:Node {name:'d'})\n" +
                    "CREATE" +
                    " (a)-[:TYPE]->(b),\n" +
                    " (b)-[:TYPE]->(c),\n" +
                    " (c)-[:TYPE]->(a),\n" +
                    " (a)-[:TYPE]->(c)";

    public static final Label LABEL = Label.label("Node");
    public static final String ABCD = "abcd";

    @Rule
    public ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private HugeGraph graph;
    private final Map<String, Integer> nameMap;

    public HugeLouvainTest1() {
        nameMap = new HashMap<>();
    }

    private void setup(String cypher) {
        DB.execute(cypher);
        graph = (HugeGraph) new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withOptionalRelationshipWeightsFromProperty("w", 1.0)
                //.withDirection(Direction.BOTH)
                .asUndirected(true)
                .load(HugeGraphFactory.class);

        try (Transaction transaction = DB.beginTx()) {
            for (int i = 0; i < ABCD.length(); i++) {
                final String value = String.valueOf(ABCD.charAt(i));
                final int id = graph.toMappedNodeId(DB.findNode(LABEL, "name", value).getId());
                nameMap.put(value, id);
            }
            transaction.success();
        }
    }

    public void printCommunities(HugeLouvain louvain) {
        try (Transaction transaction = DB.beginTx()) {
            louvain.resultStream()
                    .forEach(r -> {
                        System.out.println(DB.getNodeById(r.nodeId).getProperty("name") + ":" + r.community);
                    });
            transaction.success();
        }
    }

    @Test
    public void testRunner() throws Exception {
        setup(unidirectional);
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
        assertCommunities(algorithm);
    }

    @Test
    public void testRandomNeighborLouvain() throws Exception {
        setup(unidirectional);
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
        assertCommunities(algorithm);
    }

    public void assertCommunities(HugeLouvain louvain) {
        assertUnion(new String[]{"a", "b", "c"}, louvain.getCommunityIds());
        assertDisjoint(new String[]{"a", "d"}, louvain.getCommunityIds());
    }

    private void assertUnion(String[] nodeNames, HugeLongArray values) {
        final int[] communityIds = toIntArray(values);
        int current = -1;
        for (String name : nodeNames) {
            if (!nameMap.containsKey(name)) {
                throw new IllegalArgumentException("unknown node name: " + name);
            }
            final int id = nameMap.get(name);
            if (current == -1) {
                current = communityIds[id];
            } else {
                assertEquals(
                        "Node " + name + " belongs to wrong community " + communityIds[id],
                        current,
                        communityIds[id]);
            }
        }
    }

    private void assertDisjoint(String[] nodeNames, HugeLongArray values) {
        final int[] communityIds = toIntArray(values);
        final IntSet set = new IntHashSet();
        for (String name : nodeNames) {
            final int communityId = communityIds[nameMap.get(name)];
            assertTrue("Node " + name + " belongs to wrong community " + communityId, set.add(communityId));
        }
    }

    public static int[] toIntArray(HugeLongArray array) {
        final long size = array.size();
        final int[] data = new int[Math.toIntExact(size)];
        Arrays.setAll(data, i -> Math.toIntExact(array.get(i)));
        return data;
    }
}
