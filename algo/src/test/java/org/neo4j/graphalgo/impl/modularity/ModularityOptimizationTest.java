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
package org.neo4j.graphalgo.impl.modularity;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.modularity.ImmutableModularityOptimizationStreamConfig;
import org.neo4j.graphalgo.modularity.ModularityOptimizationStreamConfig;
import org.neo4j.graphdb.Direction;
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
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withDirection(Direction.BOTH)
            .load(HugeGraphFactory.class);

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            Direction.BOTH,
            3,
            TOLERANCE_DEFAULT,
            null,
            1,
            10_000,
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance()
        );

        pmo.compute();

        assertEquals(0.12244, pmo.getModularity(), 0.001);
        assertCommunities(getCommunityIds(graph.nodeCount(), pmo), new long[]{0, 1, 2, 4}, new long[]{3, 5});
        assertTrue(pmo.getIterations() <= 3);
    }

    @Test
    void testWeighted() {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipProperties(
                PropertyMapping.of("weight", 1.0)
            )
            .withDirection(Direction.BOTH)
            .load(HugeGraphFactory.class);

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            Direction.BOTH,
            3,
            TOLERANCE_DEFAULT,
            null,
            3,
            2,
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance()
        );

        pmo.compute();

        assertEquals(0.4985, pmo.getModularity(), 0.001);
        assertCommunities(getCommunityIds(graph.nodeCount(), pmo), new long[]{0, 4, 5}, new long[]{1, 2, 3});
        assertTrue(pmo.getIterations() <= 3);
    }

    @Test
    void testSeedingWithBiggerSeedValues() {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withOptionalNodeProperties(
                PropertyMapping.of("seed2", -1)
            )
            .withDirection(Direction.BOTH)
            .load(HugeGraphFactory.class);

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            Direction.BOTH,
            10,
            TOLERANCE_DEFAULT,
            graph.nodeProperties("seed2"),
            1,
            100,
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance()
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
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withOptionalNodeProperties(
                PropertyMapping.of("seed1", -1)
            )
            .withDirection(Direction.BOTH)
            .load(HugeGraphFactory.class);

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            Direction.BOTH,
            10,
            TOLERANCE_DEFAULT,
            graph.nodeProperties("seed1"),
            1,
            100,
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance()
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
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withDirection(Direction.BOTH)
            .load(HugeGraphFactory.class);

        TestLog log = new TestLog();

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            Direction.BOTH,
            3,
            TOLERANCE_DEFAULT,
            null,
            3,
            2,
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            log
        );

        pmo.compute();

        assertTrue(log.containsMessage(INFO, "Modularity Optimization - Initialization finished"));
        assertTrue(log.containsMessage(INFO, "Iteration 1"));
        assertTrue(log.containsMessage(INFO, "Modularity Optimization - Finished"));
    }

    @Test
    void requireAtLeastOneIteration() {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withDirection(Direction.BOTH)
            .load(HugeGraphFactory.class);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new ModularityOptimization(
                graph,
                Direction.BOTH,
                0,
                TOLERANCE_DEFAULT,
                null,
                3,
                2,
                Pools.DEFAULT,
                AllocationTracker.EMPTY,
                NullLog.getInstance()
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
            arguments(1, 5_614_088, 8_413_120),
            arguments(4, 5_617_376, 14_413_384),
            arguments(42, 5_659_024, 90_416_728)
        );
    }
}
