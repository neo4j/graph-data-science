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
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.impl.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.impl.generator.RelationshipDistribution;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.NullLog;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.CommunityHelper.assertCommunities;
import static org.neo4j.graphalgo.CommunityHelper.assertCommunitiesWithLabels;
import static org.neo4j.graphalgo.TestLog.INFO;
import static org.neo4j.graphalgo.core.ProcedureConstants.TOLERANCE_DEFAULT;
import static org.neo4j.graphalgo.graphbuilder.TransactionTerminationTestUtils.assertTerminates;

class LouvainTest extends AlgoTestBase {

    static final Louvain.Config DEFAULT_CONFIG = new Louvain.Config(
        10,
        10,
        TOLERANCE_DEFAULT,
        true,
        Optional.empty()
    );
    static final Louvain.Config DEFAULT_CONFIG_WITH_SEED = new Louvain.Config(
        10,
        10,
        TOLERANCE_DEFAULT,
        true,
        Optional.of("seed")
    );

    Direction direction;

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {seed: 1})" +        // 0
        ", (b:Node {seed: 1})" +        // 1
        ", (c:Node {seed: 1})" +        // 2
        ", (d:Node {seed: 1})" +        // 3
        ", (e:Node {seed: 1})" +        // 4
        ", (f:Node {seed: 1})" +        // 5
        ", (g:Node {seed: 2})" +        // 6
        ", (h:Node {seed: 2})" +        // 7
        ", (i:Node {seed: 2})" +        // 8
        ", (j:Node {seed: 42})" +       // 9
        ", (k:Node {seed: 42})" +       // 10
        ", (l:Node {seed: 42})" +       // 11
        ", (m:Node {seed: 42})" +       // 12
        ", (n:Node {seed: 42})" +       // 13
        ", (x:Node {seed: 1})" +        // 14
        ", (u:Some)" +
        ", (v:Other)" +
        ", (w:Label)" +

        ", (a)-[:TYPE {weight: 1.0}]->(b)" +
        ", (a)-[:TYPE {weight: 1.0}]->(d)" +
        ", (a)-[:TYPE {weight: 1.0}]->(f)" +
        ", (b)-[:TYPE {weight: 1.0}]->(d)" +
        ", (b)-[:TYPE {weight: 1.0}]->(x)" +
        ", (b)-[:TYPE {weight: 1.0}]->(g)" +
        ", (b)-[:TYPE {weight: 1.0}]->(e)" +
        ", (c)-[:TYPE {weight: 1.0}]->(x)" +
        ", (c)-[:TYPE {weight: 1.0}]->(f)" +
        ", (d)-[:TYPE {weight: 1.0}]->(k)" +
        ", (e)-[:TYPE {weight: 1.0}]->(x)" +
        ", (e)-[:TYPE {weight: 0.01}]->(f)" +
        ", (e)-[:TYPE {weight: 1.0}]->(h)" +
        ", (f)-[:TYPE {weight: 1.0}]->(g)" +
        ", (g)-[:TYPE {weight: 1.0}]->(h)" +
        ", (h)-[:TYPE {weight: 1.0}]->(i)" +
        ", (h)-[:TYPE {weight: 1.0}]->(j)" +
        ", (i)-[:TYPE {weight: 1.0}]->(k)" +
        ", (j)-[:TYPE {weight: 1.0}]->(k)" +
        ", (j)-[:TYPE {weight: 1.0}]->(m)" +
        ", (j)-[:TYPE {weight: 1.0}]->(n)" +
        ", (k)-[:TYPE {weight: 1.0}]->(m)" +
        ", (k)-[:TYPE {weight: 1.0}]->(l)" +
        ", (l)-[:TYPE {weight: 1.0}]->(n)" +
        ", (m)-[:TYPE {weight: 1.0}]->(n)";

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void shutdownGraphDb() {
        if (db != null) db.shutdown();
    }

    @AllGraphTypesTest
    void unweightedLouvain(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);

        Louvain algorithm = new Louvain(
            graph,
            DEFAULT_CONFIG,
            direction,
            Pools.DEFAULT,
            1,
            NullLog.getInstance(),
            AllocationTracker.EMPTY
        ).withProgressLogger(TestProgressLogger.INSTANCE).withTerminationFlag(TerminationFlag.RUNNING_TRUE);

        algorithm.compute();

        final HugeLongArray[] dendrogram = algorithm.dendrograms();
        final double[] modularities = algorithm.modularities();

        assertCommunities(
            dendrogram[0],
            new long[]{0, 1, 3},
            new long[]{2, 4, 5, 14},
            new long[]{6, 7, 8},
            new long[]{9, 10, 11, 12, 13}
        );

        assertCommunities(
            dendrogram[1],
            new long[]{0, 1, 2, 3, 4, 5, 14},
            new long[]{6, 7, 8},
            new long[]{9, 10, 11, 12, 13}
        );

        assertEquals(2, algorithm.levels());
        assertEquals(0.38, modularities[modularities.length - 1], 0.01);
    }

    @AllGraphTypesTest
    void weightedLouvain(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER, true);

        Louvain algorithm = new Louvain(
            graph,
            DEFAULT_CONFIG,
            direction,
            Pools.DEFAULT,
            1,
            NullLog.getInstance(),
            AllocationTracker.EMPTY
        ).withProgressLogger(TestProgressLogger.INSTANCE).withTerminationFlag(TerminationFlag.RUNNING_TRUE);

        algorithm.compute();

        final HugeLongArray[] dendrogram = algorithm.dendrograms();
        final double[] modularities = algorithm.modularities();

        assertCommunities(
            dendrogram[0],
            new long[]{0, 1, 3},
            new long[]{2, 4, 14},
            new long[]{5, 6},
            new long[]{7, 8},
            new long[]{9, 10, 11, 12, 13}
        );

        assertCommunities(
            dendrogram[1],
            new long[]{0, 1, 2, 3, 4, 5, 6, 14},
            new long[]{7, 8, 9, 10, 11, 12, 13}
        );

        assertEquals(2, algorithm.levels());
        assertEquals(0.37, modularities[modularities.length - 1], 0.01);
    }

    @AllGraphTypesTest
    void seededLouvain(Class<? extends GraphFactory> graphImpl) {
        assumeFalse(graphImpl == CypherGraphFactory.class);

        Graph graph = loadGraph(graphImpl, DB_CYPHER, true, "seed");

        Louvain algorithm = new Louvain(
            graph,
            DEFAULT_CONFIG_WITH_SEED,
            direction,
            Pools.DEFAULT,
            1,
            NullLog.getInstance(),
            AllocationTracker.EMPTY
        ).withProgressLogger(TestProgressLogger.INSTANCE).withTerminationFlag(TerminationFlag.RUNNING_TRUE);

        algorithm.compute();

        final HugeLongArray[] dendrogram = algorithm.dendrograms();
        final double[] modularities = algorithm.modularities();

        Map<Long, Long[]> expectedCommunitiesWithlabels = MapUtil.genericMap(
            new HashMap<>(),
            1L, new Long[]{0L, 1L, 2L, 3L, 4L, 5L, 14L},
            2L, new Long[]{6L, 7L, 8L},
            42L, new Long[]{9L, 10L, 11L, 12L, 13L}
        );

        assertCommunitiesWithLabels(
            dendrogram[0],
            expectedCommunitiesWithlabels
        );

        assertEquals(1, algorithm.levels());
        assertEquals(0.38, modularities[modularities.length - 1], 0.01);
    }

    @AllGraphTypesTest
    void testTolerance(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);

        Louvain algorithm = new Louvain(
            graph,
            new Louvain.Config(
                10,
                10,
                2.0,
                false,
                Optional.empty()
            ),
            direction,
            Pools.DEFAULT,
            1,
            NullLog.getInstance(),
            AllocationTracker.EMPTY
        ).withProgressLogger(TestProgressLogger.INSTANCE).withTerminationFlag(TerminationFlag.RUNNING_TRUE);

        algorithm.compute();

        assertEquals(1, algorithm.levels());
    }

    @AllGraphTypesTest
    void testMaxLevels(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);

        Louvain algorithm = new Louvain(
            graph,
            new Louvain.Config(
                1,
                10,
                TOLERANCE_DEFAULT,
                false,
                Optional.empty()
            ),
            direction,
            Pools.DEFAULT,
            1,
            NullLog.getInstance(),
            AllocationTracker.EMPTY
        ).withProgressLogger(TestProgressLogger.INSTANCE).withTerminationFlag(TerminationFlag.RUNNING_TRUE);

        algorithm.compute();

        assertEquals(1, algorithm.levels());
    }

    @ParameterizedTest
    @MethodSource("memoryEstimationTuples")
    void testMemoryEstimation(int concurrency, int levels, long min, long max) {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100_000L).setMaxRelCount(500_000L).build();

        Louvain.Config config = new Louvain.Config(levels, 10, TOLERANCE_DEFAULT, false, Optional.empty());
        MemoryTree memoryTree = new LouvainFactory(config).memoryEstimation(dimensions, concurrency);
        assertEquals(min, memoryTree.memoryUsage().min);
        assertEquals(max, memoryTree.memoryUsage().max);
    }

    @Test
    void testCanBeInterruptedByTxCancelation() {
        Graph graph = new RandomGraphGenerator(
            100_000,
            10,
            RelationshipDistribution.UNIFORM,
            Optional.empty(),
            AllocationTracker.EMPTY
        ).generate();

        assertTerminates((terminationFlag) -> {
            new Louvain(
                graph,
                DEFAULT_CONFIG,
                Direction.BOTH,
                Pools.DEFAULT,
                2,
                NullLog.getInstance(),
                AllocationTracker.EMPTY
            )
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(terminationFlag)
                .compute();
        }, 1000, 1000);
    }

    @Test
    void testLogging() {
        Graph graph = loadGraph(HugeGraphFactory.class, DB_CYPHER);

        TestLog log = new TestLog();

        Louvain louvain = new Louvain(
            graph,
            DEFAULT_CONFIG,
            Direction.BOTH,
            Pools.DEFAULT,
            1,
            log,
            AllocationTracker.EMPTY
        );

        louvain.compute();

        assertTrue(log.containsMessage(INFO, "Louvain - Level 1 finished after"));
        assertTrue(log.containsMessage(INFO, "Louvain - Finished after"));
    }

    static Stream<Arguments> memoryEstimationTuples() {
        return Stream.of(
            arguments(1, 1, 6_414_193, 16_338_904),
            arguments(1, 10, 6_414_193, 23_539_264),
            arguments(4, 1, 6_417_481, 22_143_280),
            arguments(4, 10, 6_417_481, 29_343_640),
            arguments(42, 1, 6_459_129, 98_116_768),
            arguments(42, 10, 6_459_129, 105_317_128)
        );
    }

    private Graph loadGraph(Class<? extends GraphFactory> graphImpl, String cypher, String... nodeProperties) {
        return loadGraph(graphImpl, cypher, false, nodeProperties);
    }

    private Graph loadGraph(
        Class<? extends GraphFactory> graphImpl,
        String cypher,
        boolean loadRelWeight,
        String... nodeProperties
    ) {
        runQuery(cypher);
        GraphLoader loader = new GraphLoader(db)
            .withOptionalNodeProperties(
                Arrays.stream(nodeProperties)
                    .map(p -> PropertyMapping.of(p, -1))
                    .toArray(PropertyMapping[]::new)
            );
        if (loadRelWeight) {
            loader.withRelationshipProperties(PropertyMapping.of("weight", 1.0));
        }
        if (graphImpl == CypherGraphFactory.class) {
            direction = Direction.OUTGOING;
            loader
                .withNodeStatement("MATCH (u:Node) RETURN id(u) as id, u.seed1 as seed1, u.seed2 as seed2")
                .withRelationshipStatement("MATCH (u1:Node)-[rel]-(u2:Node) \n" +
                                           "RETURN id(u1) AS source, id(u2) AS target, rel.weight as weight")
                .withDeduplicationStrategy(DeduplicationStrategy.NONE)
                .undirected();
        } else {
            direction = Direction.BOTH;
            loader
                .withAnyRelationshipType()
                .withLabel("Node");
        }
        loader.withDirection(direction);
        try (Transaction tx = db.beginTx()) {
            return loader.load(graphImpl);
        }
    }
}
