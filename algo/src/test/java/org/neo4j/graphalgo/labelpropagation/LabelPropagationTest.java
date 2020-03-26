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
package org.neo4j.graphalgo.labelpropagation;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.CypherLoaderBuilder;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.CypherFactory;
import org.neo4j.graphalgo.core.loading.NativeFactory;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.applyInTransaction;
import static org.neo4j.graphalgo.compat.MapUtil.genericMap;
import static org.neo4j.graphalgo.compat.MapUtil.map;

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

    private final Log log = new TestLog();

    Graph loadGraph(Class<? extends GraphStoreFactory> graphImpl) {
        GraphLoader graphLoader;
        if (graphImpl == CypherFactory.class) {
            graphLoader = new CypherLoaderBuilder()
                .api(db)
                .nodeQuery("MATCH (u:User) RETURN id(u) AS id")
                .relationshipQuery("MATCH (u1:User)-[rel:FOLLOW]->(u2:User) \n" +
                                   "RETURN id(u1) AS source, id(u2) AS target")
                .build();
        } else {
            graphLoader = new StoreLoaderBuilder()
                .api(db)
                .addNodeLabel("User")
                .addRelationshipType("FOLLOW")
                .build();
        }

        return applyInTransaction(db, tx -> graphLoader.load(graphImpl));
    }

    @AllGraphTypesTest
    void testUsesNeo4jNodeIdWhenSeedPropertyIsMissing(Class<? extends GraphStoreFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl);
        LabelPropagation lp = new LabelPropagation(
            graph,
            ImmutableLabelPropagationStreamConfig.builder().maxIterations(1).build(),
            Pools.DEFAULT,
            progressLogger,
            AllocationTracker.EMPTY
        );
        lp.compute();
        HugeLongArray labels = lp.labels();
        assertArrayEquals(new long[]{1, 1, 3, 4, 4, 1}, labels.toArray(), "Incorrect result assuming initial labels are neo4j id");
    }

    @Test
    void shouldWorkWithSeedOnExplicitlyLoadedGraph() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("User")
            .addRelationshipType("FOLLOW")
            .addNodeProperty(PropertyMapping.of("seedId", 0.0))
            .build()
            .graph(NativeFactory.class);

        LabelPropagation lp = new LabelPropagation(
            graph,
            ImmutableLabelPropagationStreamConfig
                .builder()
                .seedProperty("seedId")
                .maxIterations(1)
                .build(),
            Pools.DEFAULT,
            progressLogger,
            AllocationTracker.EMPTY
        );

        HugeLongArray labels = lp.compute().labels();

        assertArrayEquals(new long[]{2, 2, 3, 4, 4, 2}, labels.toArray());
    }

    @AllGraphTypesTest
    void testSingleThreadClustering(Class<? extends GraphStoreFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl);
        testClustering(graph, 100);
    }

    @AllGraphTypesTest
    void testMultiThreadClustering(Class<? extends GraphStoreFactory> graphImpl) {
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
            progressLogger,
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
        GraphDimensions largeDimensions = ImmutableGraphDimensions.builder()
            .nodeCount((long) Integer.MAX_VALUE + (long) Integer.MAX_VALUE)
            .build();

        // test for no failure and no overflow
        assertTrue(0 < labelPropagation
            .memoryEstimation(ImmutableLabelPropagationStreamConfig.builder().build())
            .estimate(largeDimensions, 1)
            .memoryUsage().max);
    }

    @Test
    void shouldLogProgress(){
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("User")
            .addRelationshipType("FOLLOW")
            .build()
            .graph(NativeFactory.class);

        TestProgressLogger testLogger = new TestProgressLogger(graph.relationshipCount(), "Louvain");

        LabelPropagation lp = new LabelPropagation(
            graph,
            defaultConfig(),
            Pools.DEFAULT,
            testLogger,
            AllocationTracker.EMPTY
        );

        lp.compute();

        List<AtomicLong> progresses = testLogger.getProgresses();

        // Should log progress for every iteration + init step
        assertEquals(defaultConfig().maxIterations() + 2, progresses.size());
        progresses.forEach(progress -> assertTrue(progress.get() <= graph.relationshipCount()));

        assertTrue(testLogger.containsMessage(TestLog.INFO, ":: Start"));
        LongStream.range(1, lp.ranIterations() + 1).forEach(iteration -> {
            assertTrue(testLogger.containsMessage(TestLog.INFO, String.format("Iteration %d :: Start", iteration)));
            assertTrue(testLogger.containsMessage(TestLog.INFO, String.format("Iteration %d :: Start", iteration)));
        });
        assertTrue(testLogger.containsMessage(TestLog.INFO, ":: Finished"));
    }

    private void assertMemoryEstimation(long nodeCount, int concurrency) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();

        LabelPropagationFactory<LabelPropagationBaseConfig> labelPropagation = new LabelPropagationFactory<>(defaultConfig());

        MemoryRange actual = labelPropagation
            .memoryEstimation(ImmutableLabelPropagationStreamConfig.builder().build())
            .estimate(dimensions, concurrency)
            .memoryUsage();

        Map<Integer, Long> minByConcurrency = genericMap(
            1, 800488L,
            4, 801592L,
            42, 815576L
        );

        Map<Integer, Long> maxByConcurrency = genericMap(
            1, 4994664L,
            4, 17578296L,
            42, 176970968L
        );

        assertEquals(minByConcurrency.get(concurrency), actual.min, "min");
        assertEquals(maxByConcurrency.get(concurrency), actual.max, "max");
    }

    LabelPropagationStreamConfig defaultConfig() {
        return ImmutableLabelPropagationStreamConfig.builder().build();
    }
}
