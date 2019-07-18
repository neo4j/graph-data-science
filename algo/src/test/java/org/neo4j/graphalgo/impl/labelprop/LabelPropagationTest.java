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
package org.neo4j.graphalgo.impl.labelprop;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public final class LabelPropagationTest {

    private static final String GRAPH =
            "CREATE (nAlice:User {id:'Alice',seedId:2})\n" +
            ",(nBridget:User {id:'Bridget',seedId:3})\n" +
            ",(nCharles:User {id:'Charles',seedId:4})\n" +
            ",(nDoug:User {id:'Doug',seedId:3})\n" +
            ",(nMark:User {id:'Mark',seedId: 4})\n" +
            ",(nMichael:User {id:'Michael',seedId:2})\n" +
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

    @Rule
    public final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private final Class<? extends GraphFactory> graphImpl;
    private Graph graph;

    public LabelPropagationTest(Class<? extends GraphFactory> graphImpl) {
        this.graphImpl = graphImpl;
    }

    @Before
    public void setup() {
        DB.execute(GRAPH).close();
        GraphLoader graphLoader = new GraphLoader(DB, Pools.DEFAULT)
                .withDirection(Direction.OUTGOING)
                .withConcurrency(Pools.DEFAULT_CONCURRENCY);

        if (graphImpl == HeavyCypherGraphFactory.class) {
            graphLoader
                    .withLabel("MATCH (u:User) RETURN id(u) AS id")
                    .withRelationshipType("MATCH (u1:User)-[rel:FOLLOW]->(u2:User) \n" +
                                          "RETURN id(u1) AS source, id(u2) AS target")
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
    public void testGeneratedAndProvidedLabelsDontConflict() {
        int seededLabel = 1;
        // When
        String query = "CREATE " +
                       " (a:Pet {type: 'cat',   seedId: $seed}) " +
                       ",(b:Pet {type: 'okapi', seedId: $seed}) " +
                       ",(c:Pet {type: 'koala'}) " +
                       ",(d:Pet {type: 'python'}) " +
                       ",(a)<-[:REL]-(c) " +
                       ",(b)<-[:REL]-(c) " +
                       "RETURN id(d) AS maxId";

        // (c) will get seed 1
        // (d) will get seed id(d) + 1
        long maxId = (Long) DB.execute(query, Collections.singletonMap("seed", seededLabel)).next().get("maxId");

        GraphLoader graphLoader = new GraphLoader(DB, Pools.DEFAULT)
                .withOptionalNodeProperties(new PropertyMapping(LabelPropagation.SEED_TYPE, "seedId", 0.0))
                .withDirection(Direction.OUTGOING)
                .withConcurrency(Pools.DEFAULT_CONCURRENCY);

        if (graphImpl == HeavyCypherGraphFactory.class) {
            graphLoader
                    .withLabel("MATCH (u:Pet) RETURN id(u) AS id, u.seedId AS seedId")
                    .withRelationshipType("MATCH (u1:Pet)-[rel:REL]->(u2:Pet) " +
                                          "RETURN id(u1) AS source, id(u2) AS target")
                    .withName("cypher");
        } else {
            graphLoader
                    .withLabel("Pet")
                    .withRelationshipType("REL")
                    .withName(graphImpl.getSimpleName());
        }
        graph = graphLoader.load(graphImpl);
        LabelPropagation lp = new LabelPropagation(
                graph,
                10000,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT,
                AllocationTracker.EMPTY
        );
        lp.compute(Direction.OUTGOING, 1L);
        HugeLongArray labels = lp.labels();
        assertArrayEquals("Incorrect result assuming initial labels are neo4j id", new long[]{1, 1, 1, maxId + seededLabel + 1}, labels.toArray());
    }

    @Test
    public void testUsesNeo4jNodeIdWhenSeedPropertyIsMissing() {
        LabelPropagation lp = new LabelPropagation(
                graph,
                10000,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT,
                AllocationTracker.EMPTY
        );
        lp.compute(Direction.OUTGOING, 1L);
        HugeLongArray labels = lp.labels();
        assertArrayEquals("Incorrect result assuming initial labels are neo4j id", new long[]{1, 1, 3, 4, 4, 1}, labels.toArray());
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
        testClustering(100);
    }

    @Test
    public void testHugeMultiThreadClustering() {
        testClustering(2);
    }

    private void testClustering(int batchSize) {
        for (int i = 0; i < 20; i++) {
            testLPClustering(batchSize);
        }
    }

    private void testLPClustering(int batchSize) {
        LabelPropagation lp = new LabelPropagation(
                graph,
                batchSize,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT,
                AllocationTracker.EMPTY
        );
        lp.compute(Direction.OUTGOING, 10L);
        HugeLongArray labels = lp.labels();
        assertNotNull(labels);
        IntObjectMap<IntArrayList> cluster = groupByPartitionInt(labels);
        assertNotNull(cluster);

        assertTrue(lp.didConverge());
        assertTrue("expected at least 2 iterations, got " + lp.ranIterations(), 2L <= lp.ranIterations());
        assertEquals(2L, (long) cluster.size());
        for (IntObjectCursor<IntArrayList> cursor : cluster) {
            int[] ids = cursor.value.toArray();
            Arrays.sort(ids);
            if (cursor.key == 0 || cursor.key == 1 || cursor.key == 5) {
                assertArrayEquals(new int[]{0, 1, 5}, ids);
            } else {
                assertArrayEquals(new int[]{2, 3, 4}, ids);
            }
        }
    }

    private static IntObjectMap<IntArrayList> groupByPartitionInt(HugeLongArray labels) {
        if (labels == null) {
            return null;
        }
        IntObjectMap<IntArrayList> cluster = new IntObjectHashMap<>();
        for (int node = 0, l = Math.toIntExact(labels.size()); node < l; node++) {
            int key = Math.toIntExact(labels.get(node));
            IntArrayList ids = cluster.get(key);
            if (ids == null) {
                ids = new IntArrayList();
                cluster.put(key, ids);
            }
            ids.add(node);
        }

        return cluster;
    }
}
