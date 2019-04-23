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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeNodeProperties;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.LabelPropagationAlgorithm.Labels;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphalgo.impl.LabelPropagationAlgorithm.PARTITION_TYPE;
import static org.neo4j.graphalgo.impl.LabelPropagationAlgorithm.WEIGHT_TYPE;

//@formatter:off
/**
 *
 *                                         +-----+
 *                                   +---->+  B  |
 *                                   |     |  1  |
 *                                   |     +-+---+
 *                                   v       | ^
 *  +-----+     +---+    +---+     +-+-+     | |
 *  |  Ma +<--->+ D |<---+ C |<----+ A |     | |
 *  |  4  |     | 3 |    | 2 |     | 0 |     | |
 *  +-----+     +---+    +---+     +-+-+     | |
 *                                   ^       v |
 *                                   |     +---+-+
 *                                   +---->| Mic |
 *                                         |  5  |
 *                                         +-----+
 *
 * Ideally, the iterations would go like this.
 *
 * 1st iteration:
 *   A   -> Mic
 *   B   -> Mic
 *   C   -> D/Ma
 *   D   -> Ma
 *   Ma  -> Ma/D
 *   Mic -> B
 *
 * 2nd iteration:
 *   A   -> Mic
 *   B   -> Mic
 *   C   -> Ma
 *   D   -> Ma
 *   Ma  -> Ma
 *   Mic -> Mic
 *
 * 3rd iteration:
 *  nothing to do, finished
 */
//@formatter:on
@RunWith(Parameterized.class)
public final class LabelPropagation420Test {

    private static final String GRAPH =
            "CREATE (nAlice:User {id:'Alice',label:2})\n" +
                    ",(nBridget:User {id:'Bridget',label:3})\n" +
                    ",(nCharles:User {id:'Charles',label:4})\n" +
                    ",(nDoug:User {id:'Doug',label:3})\n" +
                    ",(nMark:User {id:'Mark',label: 4})\n" +
                    ",(nMichael:User {id:'Michael',label:2})\n" +
                    "CREATE (nAlice)-[:FOLLOW]->(nBridget)\n" +
                    ",(nAlice)-[:FOLLOW]->(nCharles)\n" +
                    ",(nMark)-[:FOLLOW]->(nDoug)\n" +
                    ",(nBridget)-[:FOLLOW]->(nMichael)\n" +
                    ",(nDoug)-[:FOLLOW]->(nMark)\n" +
                    ",(nMichael)-[:FOLLOW]->(nAlice)\n" +
                    ",(nAlice)-[:FOLLOW]->(nMichael)\n" +
                    ",(nBridget)-[:FOLLOW]->(nAlice)\n" +
                    ",(nMichael)-[:FOLLOW]->(nBridget)\n" +
                    ",(nCharles)-[:FOLLOW]->(nDoug)";

    @Parameterized.Parameters(name = "graph={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class},
                new Object[]{HeavyCypherGraphFactory.class},
                new Object[]{HugeGraphFactory.class}
        );
    }

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() {
        DB.execute(GRAPH).close();
    }

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private final Class<? extends GraphFactory> graphImpl;
    private Graph graph;

    public LabelPropagation420Test(Class<? extends GraphFactory> graphImpl) {
        this.graphImpl = graphImpl;
    }

    @Before
    public void setup() {
        GraphLoader graphLoader = new GraphLoader(DB, Pools.DEFAULT)
                .withRelationshipWeightsFromProperty("weight", 1.0)
                .withOptionalNodeProperties(
                        PropertyMapping.of(WEIGHT_TYPE, WEIGHT_TYPE, 1.0),
                        PropertyMapping.of(PARTITION_TYPE, PARTITION_TYPE, 0.0)
                )
                .withDirection(Direction.BOTH)
                .withConcurrency(Pools.DEFAULT_CONCURRENCY);

        if (graphImpl == HeavyCypherGraphFactory.class) {
            graphLoader
                    .withLabel("MATCH (u:User) RETURN id(u) as id")
                    .withRelationshipType("MATCH (u1:User)-[rel:FOLLOW]->(u2:User) \n" +
                            "RETURN id(u1) as source,id(u2) as target")
                    .withName("cypher");
        } else {
            graphLoader
                    .withLabel("User")
                    .withRelationshipType("FOLLOW")
                    .withName(graphImpl.getSimpleName());
        }
        graph = graphLoader.load(graphImpl);
    }

    @Test
    public void testSingleThreadClustering() {
        testClustering(100);
    }

    @Test
    public void testMultiThreadClustering() {
        testClustering(2);
    }

    @Test
    public void testHugeSingleThreadClustering() {
        testHugeClustering(100);
    }

    @Test
    public void testHugeMultiThreadClustering() {
        testHugeClustering(2);
    }

    private void testClustering(int batchSize) {
        testClustering(new LabelPropagation(
                graph,
                (NodeProperties) graph,
                batchSize,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT));
    }

    private void testHugeClustering(int batchSize) {
        if (graph instanceof HugeGraph) {
            testClustering(new HugeLabelPropagation(
                    (HugeGraph) graph,
                    (HugeNodeProperties) graph,
                    batchSize,
                    Pools.DEFAULT_CONCURRENCY,
                    Pools.DEFAULT,
                    AllocationTracker.EMPTY));
        }
    }

    // possible bad seed: -2300107887844480632
    private void testClustering(LabelPropagationAlgorithm<?> lp) {
        Long seed = Long.getLong("tests.seed");
        if (seed != null) {
            lp.compute(Direction.OUTGOING, 10L, seed);
        } else {
            lp.compute(Direction.OUTGOING, 10L);
        }
        Labels labels = lp.labels();
        assertNotNull(labels);
        IntObjectMap<IntArrayList> cluster = LabelPropagationTests.groupByPartitionInt(labels);
        assertNotNull(cluster);

        // It could happen that the labels for Charles, Doug, and Mark oscillate,
        // i.e they assign each others' label in every iteration and the graph won't converge.
        // LPA runs asynchronous and shuffles the order of iteration a bit to try
        // to minimize the oscillations, but it cannot be guaranteed that
        // it will never happen. It's RNG after all: http://dilbert.com/strip/2001-10-25
        if (lp.didConverge()) {
            assertTrue("expected at least 2 iterations, got " + lp.ranIterations(), 2L <= lp.ranIterations());
            assertEquals(2L, (long) cluster.size());
            for (IntObjectCursor<IntArrayList> cursor : cluster) {
                int[] ids = cursor.value.toArray();
                Arrays.sort(ids);
                if (cursor.key == 0 || cursor.key == 1 || cursor.key == 5) {
                    assertArrayEquals(new int[]{0, 1, 5}, ids);
                } else if (cursor.key == 2) {
                    if (ids[0] == 0) {
                        assertArrayEquals(new int[]{0, 1, 5}, ids);
                    } else {
                        assertArrayEquals(new int[]{2, 3, 4}, ids);
                    }
                }
            }
        } else {
            assertEquals((long) 10, lp.ranIterations());
            System.out.println("non-converged cluster = " + cluster);
            IntArrayList cluster5 = cluster.get(5);
            assertNotNull(cluster5);
            int[] ids = cluster5.toArray();
            Arrays.sort(ids);
            assertArrayEquals(new int[]{0, 1, 5}, ids);
        }
    }
}
