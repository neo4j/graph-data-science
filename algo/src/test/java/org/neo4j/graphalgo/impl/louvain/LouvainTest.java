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

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.neo4j.graphalgo.HeavyHugeTester;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * (a)-(b)-(d)
 * | /            -> (abc)-(d)
 * (c)
 *
 * @author mknblch
 */
public class LouvainTest extends HeavyHugeTester {

    static Louvain.Config DEFAULT_CONFIG = new Louvain.Config(10, 10, false);

    private static final String unidirectional =
            "CREATE (a:Node {name:'a'})\n" +
                    "CREATE (b:Node {name:'b'})\n" +
                    "CREATE (c:Node {name:'c'})\n" +
                    "CREATE (d:Node {name:'d'})\n" +
                    "CREATE" +
                    " (a)-[:TYPE {weight: 1.0}]->(b),\n" +
                    " (b)-[:TYPE {weight: 1.0}]->(c),\n" +
                    " (c)-[:TYPE {weight: 1.0}]->(a),\n" +
                    " (a)-[:TYPE {weight: 1.0}]->(c)";

    public static final Label LABEL = Label.label("Node");
    public static final String ABCD = "abcd";

    @Rule
    public ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private Graph graph;
    private final Map<String, Integer> nameMap;

    public LouvainTest(Class<? extends GraphFactory> graphImpl, String name) {
        super(graphImpl);
        nameMap = new HashMap<>();
    }

    private void setup(String cypher) {
        DB.execute(cypher);
        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withOptionalRelationshipWeightsFromProperty("weight", 1.0)
                //.withDirection(Direction.BOTH)
                .asUndirected(true)
                .load(graphImpl);

        try (Transaction transaction = DB.beginTx()) {
            for (int i = 0; i < ABCD.length(); i++) {
                final String value = String.valueOf(ABCD.charAt(i));
                final long id = graph.toMappedNodeId(DB.findNode(LABEL, "name", value).getId());
                nameMap.put(value, (int) id);
            }
            transaction.success();
        }
    }

    @Test
    public void testRunner() throws Exception {
        setup(unidirectional);
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
        assertCommunities(algorithm);
    }

    @Test
    public void testRandomNeighborLouvain() throws Exception {
        setup(unidirectional);
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
        assertCommunities(algorithm);
    }

    @Test
    public void testMultithreadedLouvain() {
        GraphBuilder.create(DB)
                .setLabel("Node")
                .setRelationship("REL")
                .newCompleteGraphBuilder()
                .createCompleteGraph(200, 1.0);
        graph = new GraphLoader(DB)
                .withLabel("Node")
                .withRelationshipType("REL")
                .withOptionalRelationshipWeightsFromProperty(null, 1.0)
                .withoutNodeWeights()
                .withSort(true)
                .asUndirected(true)
                .load(graphImpl);

        Louvain.Config config = new Louvain.Config(99, 99999);
        Louvain algorithm = new Louvain(graph, config, Pools.DEFAULT, 4, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute(99, 99999);

        assertEquals(1, algorithm.getLevel());
        long[] expected = new long[200];
        HugeLongArray[] dendrogram = algorithm.getDendrogram();
        for (HugeLongArray communities : dendrogram) {
            long[] communityIds = communities.toArray();
            assertArrayEquals(expected, communityIds);
        }
    }

    @Test
    public void testMemoryEstimationComputation() {
        LouvainFactory factory = new LouvainFactory(DEFAULT_CONFIG);

        GraphDimensions dimensions0 = new GraphDimensions.Builder().setNodeCount(0).build();
        assertEquals(MemoryRange.of(608, 1072), factory.memoryEstimation().estimate(dimensions0, 1).memoryUsage());
        assertEquals(MemoryRange.of(1112, 1576), factory.memoryEstimation().estimate(dimensions0, 4).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(MemoryRange.of(7008, 14672), factory.memoryEstimation().estimate(dimensions100, 1).memoryUsage());
        assertEquals(MemoryRange.of(14712, 22376), factory.memoryEstimation().estimate(dimensions100, 4).memoryUsage());

        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
        assertEquals(MemoryRange.of(6400976563232L, 13602075196648L), factory.memoryEstimation().estimate(dimensions100B, 1).memoryUsage());
        assertEquals(MemoryRange.of(13602075196688L, 20803173830104L), factory.memoryEstimation().estimate(dimensions100B, 4).memoryUsage());
    }

    private void assertCommunities(Louvain louvain) {
        assertUnion(new String[]{"a", "b", "c"}, louvain.getCommunityIds());
        assertDisjoint(new String[]{"a", "d"}, louvain.getCommunityIds());
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
                assertEquals(
                        "Node " + name + " belongs to wrong community " + communityIds[id],
                        current,
                        communityIds[id]);
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
