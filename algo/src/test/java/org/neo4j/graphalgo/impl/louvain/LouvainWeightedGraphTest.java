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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.ErrorCollector;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * (a)-(b)---(e)-(f)
 * | X |     | X |   (z)
 * (c)-(d)   (g)-(h)
 *
 * @author mknblch
 */
public class LouvainWeightedGraphTest {

    private static final String unidirectional = "CREATE " +
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

            ", (e)-[:TYPE {w: 4}]->(b)";

    private static final int MAX_ITERATIONS = 10;
    public static final Label LABEL = Label.label("Node");
    public static final String ABCDEFGHZ = "abcdefghz";

    private static final Louvain.Config DEFAULT_CONFIG = new Louvain.Config(10, 10, false);
    private static GraphDatabaseAPI DB;

    static Stream<Class<? extends GraphFactory>> parameters() {
        return Stream.of(
                HeavyGraphFactory.class,
                HugeGraphFactory.class,
                GraphViewFactory.class
        );
    }

    private final Map<String, Integer> nameMap = new HashMap<>();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    @BeforeEach
    void setup() {
        DB = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void teardown() {
        if (null != DB) {
            DB.shutdown();
            DB = null;
        }
    }

    private Graph setup(Class<? extends GraphFactory> graphImpl, String cypher) {
        DB.execute(cypher);
        Graph graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withOptionalRelationshipWeightsFromProperty("w", 1.0)
                .undirected()
                .load(graphImpl);

        try (Transaction transaction = DB.beginTx()) {
            for (int i = 0; i < ABCDEFGHZ.length(); i++) {
                final String value = String.valueOf(ABCDEFGHZ.charAt(i));
                final long id = graph.toMappedNodeId(DB.findNode(LABEL, "name", value).getId());
                nameMap.put(value, (int) id);
            }
            transaction.success();
        }

        return graph;
    }

    public void printCommunities(Louvain louvain) {
        try (Transaction ignored = DB.beginTx()) {
            louvain.resultStream().forEach(r -> System.out.println(DB.getNodeById(r.nodeId).getProperty("name") + ":" + r.community));
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testWeightedLouvain(Class<? extends GraphFactory> graphImpl) {
        Graph graph = setup(graphImpl, unidirectional);
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
        assertTrue("Maximum iterations > " + MAX_ITERATIONS, louvain.getLevel() < MAX_ITERATIONS);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testWeightedRandomNeighborLouvain(Class<? extends GraphFactory> graphImpl) {
        Graph graph = setup(graphImpl, unidirectional);
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

    public void assertCommunities(Louvain louvain) {
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
