/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.labelpropagation.ImmutableLabelPropagationStreamConfig;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationBaseConfig;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationStreamConfig;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LabelPropagationTest extends AlgoTestBase {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (nAlice:User   {id: 'Alice',   seedId: 2})" +
            ", (nBridget:User {id: 'Bridget', seedId: 3})" +
            ", (nCharles:User {id: 'Charles', seedId: 4})" +
            ", (nDoug:User    {id: 'Doug',    seedId: 3})" +
            ", (nMark:User    {id: 'Mark',    seedId: 4})" +
            ", (nMichael:User {id:'Michael',  seedId: 2})" +
            ", (nAlice)-[:FOLLOW]->(nBridget)" +
            ", (nAlice)-[:FOLLOW]->(nCharles)" +
            ", (nMark)-[:FOLLOW]->(nDoug)" +
            ", (nBridget)-[:FOLLOW]->(nMichael)" +
            ", (nDoug)-[:FOLLOW]->(nMark)" +
            ", (nMichael)-[:FOLLOW]->(nAlice)" +
            ", (nAlice)-[:FOLLOW]->(nMichael)" +
            ", (nBridget)-[:FOLLOW]->(nAlice)" +
            ", (nMichael)-[:FOLLOW]->(nBridget)" +
            ", (nCharles)-[:FOLLOW]->(nDoug)";

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void shutdownGraphDb() {
        db.shutdown();
    }

    Graph loadGraph(Class<? extends GraphFactory> graphImpl) {
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT)
                .withDirection(Direction.OUTGOING)
                .withDefaultConcurrency();

        if (graphImpl == CypherGraphFactory.class) {
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
        return QueryRunner.runInTransaction(db, () -> graphLoader.load(graphImpl));
    }

    @AllGraphTypesTest
    void testUsesNeo4jNodeIdWhenSeedPropertyIsMissing(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl);
        LabelPropagation lp = new LabelPropagation(
            graph,
            ImmutableLabelPropagationStreamConfig.builder().maxIterations(1).build(),
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );
        lp.compute();
        HugeLongArray labels = lp.labels();
        assertArrayEquals(new long[]{1, 1, 3, 4, 4, 1}, labels.toArray(), "Incorrect result assuming initial labels are neo4j id");
    }

    @AllGraphTypesWithoutCypherTest
    void shouldWorkWithSeedOnExplicitlyLoadedGraph(Class<? extends GraphFactory> graphImpl) {

        Graph graph = new GraphLoader(db, Pools.DEFAULT)
            .withDirection(Direction.OUTGOING)
            .withDefaultConcurrency()
            .withLabel("User")
            .withRelationshipType("FOLLOW")
            .withOptionalNodeProperties(PropertyMapping.of("seedId", 0.0))
            .withName(graphImpl.getSimpleName()).load(graphImpl);

        LabelPropagation lp = new LabelPropagation(
            graph,
            ImmutableLabelPropagationStreamConfig
                .builder()
                .seedProperty("seedId")
                .maxIterations(1)
                .build(),
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        HugeLongArray labels = lp.compute().labels();

        assertArrayEquals(new long[]{2, 2, 3, 4, 4, 2}, labels.toArray());
    }

    @AllGraphTypesTest
    void testSingleThreadClustering(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl);
        testClustering(graph, 100);
    }

    @AllGraphTypesTest
    void testMultiThreadClustering(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl);
        testClustering(graph, 2);
    }

    private void testClustering(Graph graph, int batchSize) {
        for (int i = 0; i < 20; i++) {
            testLPClustering(graph, batchSize);
        }
    }

    private void testLPClustering(Graph graph, int batchSize) {
        LabelPropagation lp = new LabelPropagation(
                graph,
                defaultConfig(),
                Pools.DEFAULT,
                AllocationTracker.EMPTY
        );
        lp.withBatchSize(batchSize);
        lp.compute();
        HugeLongArray labels = lp.labels();
        assertNotNull(labels);
        IntObjectMap<IntArrayList> cluster = groupByPartitionInt(labels);
        assertNotNull(cluster);

        assertTrue(lp.didConverge());
        assertTrue(2L <= lp.ranIterations(), "expected at least 2 iterations, got " + lp.ranIterations());
        assertEquals(2L, cluster.size());
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

    @Test
    void shouldComputeMemoryEstimation1Thread() {
        long nodeCount = 100_000L;
        int concurrency = 1;
        assertMemoryEstimation(nodeCount, concurrency);
    }

    @Test
    void shouldComputeMemoryEstimation4Threads() {
        long nodeCount = 100_000L;
        int concurrency = 4;
        assertMemoryEstimation(nodeCount, concurrency);
    }

    @Test
    void shouldComputeMemoryEstimation42Threads() {
        long nodeCount = 100_000L;
        int concurrency = 42;
        assertMemoryEstimation(nodeCount, concurrency);
    }

    @Test
    void shouldBoundMemEstimationToMaxSupportedDegree() {
        LabelPropagationFactory<LabelPropagationStreamConfig> labelPropagation = new LabelPropagationFactory<>(
            defaultConfig());
        GraphDimensions largeDimensions = new GraphDimensions.Builder()
            .setNodeCount((long) Integer.MAX_VALUE + (long) Integer.MAX_VALUE)
            .build();

        // test for no failure and no overflow
        assertTrue(0 < labelPropagation
            .memoryEstimation(ImmutableLabelPropagationStreamConfig.builder().build())
            .estimate(largeDimensions, 1)
            .memoryUsage().max);
    }

    private void assertMemoryEstimation(long nodeCount, int concurrency) {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(nodeCount).build();

        LabelPropagationFactory<LabelPropagationBaseConfig> labelPropagation = new LabelPropagationFactory<>(defaultConfig());

        MemoryRange actual = labelPropagation
            .memoryEstimation(ImmutableLabelPropagationStreamConfig.builder().build())
            .estimate(dimensions, concurrency)
            .memoryUsage();
        long min = 80L /* LabelPropagation.class */ +
                         16L * concurrency /* StepRunner.class */ +
                         48L * concurrency /* InitStep.class */ +
                         56L * concurrency /* ComputeStep.class */ +
                         24L * concurrency /* ComputeStepConsumer.class */ +
                         HugeLongArray.memoryEstimation(nodeCount) /* labels HugeLongArray wrapper */ +
                /* LongDoubleScatterMap votes */
                         56L * concurrency /* LongDoubleScatterMap.class */ +
                         (9 * 8 + 16) * concurrency /* long[] keys */ +
                         (9 * 8 + 16) * concurrency; /* double[] values */
        long max = 80L /* LabelPropagation.class */ +
                         16L * concurrency /* StepRunner.class */ +
                         48L * concurrency /* InitStep.class */ +
                         56L * concurrency /* ComputeStep.class */ +
                         24L * concurrency /* ComputeStepConsumer.class */ +
                         HugeLongArray.memoryEstimation(nodeCount) /* labels HugeLongArray wrapper */ +
                /* LongDoubleScattermap votes */
                         56L * concurrency /* LongDoubleScatterMap.class */ +
                         ((BitUtil.nextHighestPowerOfTwo((long) (nodeCount / 0.75)) + 1) * 8 + 16) * concurrency /* long[] keys */ +
                         ((BitUtil.nextHighestPowerOfTwo((long) (nodeCount / 0.75)) + 1) * 8 + 16) * concurrency; /* double[] values */

        assertEquals(min, actual.min, "min");
        assertEquals(max, actual.max, "max");
    }

    LabelPropagationStreamConfig defaultConfig() {
        return ImmutableLabelPropagationStreamConfig.builder().build();
    }
}
