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
package org.neo4j.graphalgo.beta.modularity;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.loading.NativeFactory;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.logging.NullLog;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.CommunityHelper.assertCommunities;
import static org.neo4j.graphalgo.TestLog.INFO;
import static org.neo4j.graphalgo.core.ProcedureConstants.TOLERANCE_DEFAULT;

class ModularityOptimizationTest extends AlgoTestBase {

    public static final long[][] EXPECTED_SEED_COMMUNITIES = {new long[]{0, 1}, new long[]{2, 4}, new long[]{3, 5}};

    static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name:'a', seed1: 1, seed2: 21})" +
        ", (b:Node {name:'b'})" +
        ", (c:Node {name:'c', seed1: 2, seed2: 42})" +
        ", (d:Node {name:'d', seed1: 3, seed2: 33})" +
        ", (e:Node {name:'e', seed1: 2, seed2: 42})" +
        ", (f:Node {name:'f', seed1: 3, seed2: 33})" +
        ", (a)-[:TYPE {weight: 0.01}]->(b)" +
        ", (a)-[:TYPE {weight: 5.0}]->(e)" +
        ", (a)-[:TYPE {weight: 5.0}]->(f)" +
        ", (b)-[:TYPE {weight: 5.0}]->(c)" +
        ", (b)-[:TYPE {weight: 5.0}]->(d)" +
        ", (c)-[:TYPE {weight: 0.01}]->(e)" +
        ", (f)-[:TYPE {weight: 0.01}]->(d)";

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void shutdownGraphDb() {
        db.shutdown();
    }

    @Test
    void testUnweighted() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .putRelationshipProjectionsWithIdentifier("TYPE_OUT", RelationshipProjection.of("TYPE", Orientation.NATURAL))
            .putRelationshipProjectionsWithIdentifier("TYPE_IN", RelationshipProjection.of("TYPE", Orientation.REVERSE))
            .build()
            .graph(NativeFactory.class);

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            3,
            TOLERANCE_DEFAULT,
            null,
            1,
            10_000,
            Pools.DEFAULT,
            progressLogger,
            AllocationTracker.EMPTY
        );

        pmo.compute();

        assertEquals(0.12244, pmo.getModularity(), 0.001);
        assertCommunities(getCommunityIds(graph.nodeCount(), pmo), new long[]{0, 1, 2, 4}, new long[]{3, 5});
        assertTrue(pmo.getIterations() <= 3);
    }

    @Test
    void foo() {
        Graph graph = RandomGraphGenerator.generate(10000, 10, RelationshipDistribution.POWER_LAW);

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            3,
            TOLERANCE_DEFAULT,
            null,
            1,
            10_000,
            Pools.DEFAULT,
            progressLogger,
            AllocationTracker.EMPTY
        );

        pmo.compute();
    }

    @Test
    void testWeighted() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .putRelationshipProjectionsWithIdentifier("TYPE_OUT", RelationshipProjection.of("TYPE", Orientation.NATURAL))
            .putRelationshipProjectionsWithIdentifier("TYPE_IN", RelationshipProjection.of("TYPE", Orientation.REVERSE))
            .addRelationshipProperty(PropertyMapping.of("weight", 1.0))
            .build()
            .graph(NativeFactory.class);

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            3,
            TOLERANCE_DEFAULT,
            null,
            3,
            2,
            Pools.DEFAULT,
            progressLogger,
            AllocationTracker.EMPTY
        );

        pmo.compute();

        assertEquals(0.4985, pmo.getModularity(), 0.001);
        assertCommunities(getCommunityIds(graph.nodeCount(), pmo), new long[]{0, 4, 5}, new long[]{1, 2, 3});
        assertTrue(pmo.getIterations() <= 3);
    }

    @Test
    void testSeedingWithBiggerSeedValues() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .putRelationshipProjectionsWithIdentifier("TYPE_OUT", RelationshipProjection.of("TYPE", Orientation.NATURAL))
            .putRelationshipProjectionsWithIdentifier("TYPE_IN", RelationshipProjection.of("TYPE", Orientation.REVERSE))
            .addNodeProperty(PropertyMapping.of("seed2", -1))
            .build()
            .graph(NativeFactory.class);

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            10,
            TOLERANCE_DEFAULT,
            graph.nodeProperties("seed2"),
            1,
            100,
            Pools.DEFAULT,
            progressLogger,
            AllocationTracker.EMPTY
        );

        pmo.compute();

        long[] actualCommunities = getCommunityIds(graph.nodeCount(), pmo);
        assertEquals(0.0816, pmo.getModularity(), 0.001);
        assertCommunities(actualCommunities, EXPECTED_SEED_COMMUNITIES);
        assertTrue(actualCommunities[0] == 43 && actualCommunities[2] == 42 && actualCommunities[3] == 33);
        assertTrue(pmo.getIterations() <= 3);
    }

    @Test
    void testSeeding() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .putRelationshipProjectionsWithIdentifier("TYPE_OUT", RelationshipProjection.of("TYPE", Orientation.NATURAL))
            .putRelationshipProjectionsWithIdentifier("TYPE_IN", RelationshipProjection.of("TYPE", Orientation.REVERSE))
            .addNodeProperty(PropertyMapping.of("seed1", -1))
            .build()
            .graph(NativeFactory.class);

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            10,
            TOLERANCE_DEFAULT,
            graph.nodeProperties("seed1"),
            1,
            100,
            Pools.DEFAULT,
            progressLogger,
            AllocationTracker.EMPTY
        );

        pmo.compute();

        long[] actualCommunities = getCommunityIds(graph.nodeCount(), pmo);
        assertEquals(0.0816, pmo.getModularity(), 0.001);
        assertCommunities(actualCommunities, EXPECTED_SEED_COMMUNITIES);
        assertTrue(actualCommunities[0] == 4 && actualCommunities[2] == 2 || actualCommunities[3] == 3);
        assertTrue(pmo.getIterations() <= 3);
    }

    private long[] getCommunityIds(long nodeCount, ModularityOptimization pmo) {
        long[] communityIds = new long[(int) nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            communityIds[i] = pmo.getCommunityId(i);
        }
        return communityIds;
    }

    @Test
    void testLogging() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .putRelationshipProjectionsWithIdentifier("TYPE_OUT", RelationshipProjection.of("TYPE", Orientation.NATURAL))
            .putRelationshipProjectionsWithIdentifier("TYPE_IN", RelationshipProjection.of("TYPE", Orientation.REVERSE))
            .build()
            .graph(NativeFactory.class);

        TestProgressLogger testLogger = new TestProgressLogger(
            graph.relationshipCount(),
            "ModularityOptimization"
        );

        ModularityOptimization modularityOptimization = new ModularityOptimization(
            graph,
            3,
            TOLERANCE_DEFAULT,
            null,
            3,
            2,
            Pools.DEFAULT,
            testLogger,
            AllocationTracker.EMPTY
        );

        modularityOptimization.compute();

        assertTrue(testLogger.containsMessage(INFO, ":: Start"));
        assertTrue(testLogger.containsMessage(INFO, "Initialization :: Start"));
        assertTrue(testLogger.containsMessage(INFO, "Initialization :: Finished"));
        assertTrue(testLogger.containsMessage(INFO, "Iteration 1 :: Start"));
        assertTrue(testLogger.containsMessage(INFO, "Iteration 1 :: Finished"));
        assertTrue(testLogger.containsMessage(INFO, ":: Finished"));
    }

    @Test
    void requireAtLeastOneIteration() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .putRelationshipProjectionsWithIdentifier("TYPE_OUT", RelationshipProjection.of("TYPE", Orientation.NATURAL))
            .putRelationshipProjectionsWithIdentifier("TYPE_IN", RelationshipProjection.of("TYPE", Orientation.REVERSE))
            .build()
            .graph(NativeFactory.class);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new ModularityOptimization(
                graph,
                0,
                TOLERANCE_DEFAULT,
                null,
                3,
                2,
                Pools.DEFAULT,
                progressLogger,
                AllocationTracker.EMPTY
            )
        );

        assertTrue(exception.getMessage().contains("at least one iteration"));
    }

    @ParameterizedTest
    @MethodSource("memoryEstimationTuples")
    void testMemoryEstimation(int concurrency, long min, long max) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(100_000L).build();

        ModularityOptimizationStreamConfig config = ImmutableModularityOptimizationStreamConfig.builder()
            .username(AuthSubject.ANONYMOUS.username())
            .graphName("")
            .build();
        MemoryTree memoryTree = new ModularityOptimizationFactory<>()
            .memoryEstimation(config)
            .estimate(dimensions, concurrency);
        assertEquals(min, memoryTree.memoryUsage().min);
        assertEquals(max, memoryTree.memoryUsage().max);
    }

    static Stream<Arguments> memoryEstimationTuples() {
        return Stream.of(
            arguments(1, 5614080, 8413112),
            arguments(4, 5617368, 14413376),
            arguments(42, 5659016, 90416720)
        );
    }
}
