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
package org.neo4j.graphalgo.impl.wcc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WCCTest extends WCCBaseTest {

    private static final int SETS_COUNT = 16;
    private static final int SET_SIZE = 10;

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
        int[] setSizes = new int[SETS_COUNT];
        Arrays.fill(setSizes, SET_SIZE);
        createTestGraph(setSizes);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Override
    int communitySize() {
        return SET_SIZE;
    }

    @ParameterizedTest(name = "{0} -- {1}")
    @MethodSource("parameters")
    void shouldComputeComponents(Class<? extends GraphFactory> graphFactory, WCCType unionFindType) {
        Graph graph = new GraphLoader(db)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(graphFactory);

        WCC.Config config = new WCC.Config(null, Double.NaN);

        DisjointSetStruct result = run(unionFindType, graph, config);

        assertEquals(SETS_COUNT, getSetCount(result));
        long[] setRegions = new long[SETS_COUNT];
        Arrays.fill(setRegions, -1);

        graph.forEachNode((nodeId) -> {
            long expectedSetRegion = nodeId / SET_SIZE;
            final long setId = result.setIdOf(nodeId);
            int setRegion = (int) (setId / SET_SIZE);
            assertEquals(
                    expectedSetRegion,
                    setRegion,
                    "Node " + nodeId + " in unexpected set: " + setId);

            long regionSetId = setRegions[setRegion];
            if (regionSetId == -1) {
                setRegions[setRegion] = setId;
            } else {
                assertEquals(
                        regionSetId,
                        setId,
                        "Inconsistent set for node " + nodeId + ", is " + setId + " but should be " + regionSetId);
            }
            return true;
        });
    }

    @Test
    void memRecParallel() {
        GraphDimensions dimensions0 = new GraphDimensions.Builder().setNodeCount(0).build();

        assertEquals(
                MemoryRange.of(120),
                WCCType.PARALLEL.memoryEstimation().estimate(dimensions0, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(160),
                WCCType.PARALLEL.memoryEstimation(true).estimate(dimensions0, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(120),
                WCCType.PARALLEL.memoryEstimation().estimate(dimensions0, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(160),
                WCCType.PARALLEL.memoryEstimation(true).estimate(dimensions0, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(120),
                WCCType.PARALLEL.memoryEstimation().estimate(dimensions0, 64).memoryUsage());
        assertEquals(
                MemoryRange.of(160),
                WCCType.PARALLEL.memoryEstimation(true).estimate(dimensions0, 64).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(
                MemoryRange.of(920),
                WCCType.PARALLEL.memoryEstimation().estimate(dimensions100, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(1760),
                WCCType.PARALLEL.memoryEstimation(true).estimate(dimensions100, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(920),
                WCCType.PARALLEL.memoryEstimation().estimate(dimensions100, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(1760),
                WCCType.PARALLEL.memoryEstimation(true).estimate(dimensions100, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(920),
                WCCType.PARALLEL.memoryEstimation().estimate(dimensions100, 64).memoryUsage());
        assertEquals(
                MemoryRange.of(1760),
                WCCType.PARALLEL.memoryEstimation(true).estimate(dimensions100, 64).memoryUsage());

        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
        assertEquals(
                MemoryRange.of(800_122_070_448L),
                WCCType.PARALLEL.memoryEstimation().estimate(dimensions100B, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(1_600_244_140_816L),
                WCCType.PARALLEL.memoryEstimation(true).estimate(dimensions100B, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(800_122_070_448L),
                WCCType.PARALLEL.memoryEstimation().estimate(dimensions100B, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(1_600_244_140_816L),
                WCCType.PARALLEL.memoryEstimation(true).estimate(dimensions100B, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(800_122_070_448L),
                WCCType.PARALLEL.memoryEstimation().estimate(dimensions100B, 64).memoryUsage());
        assertEquals(
                MemoryRange.of(1_600_244_140_816L),
                WCCType.PARALLEL.memoryEstimation(true).estimate(dimensions100B, 64).memoryUsage());
    }

    @Test
    void memRecForkJoin() {
        GraphDimensions dimensions0 = new GraphDimensions.Builder().setNodeCount(0).build();

        assertEquals(
                MemoryRange.of(168),
                WCCType.FORK_JOIN.memoryEstimation().estimate(dimensions0, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(616),
                WCCType.FORK_JOIN.memoryEstimation().estimate(dimensions0, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(4200),
                WCCType.FORK_JOIN.memoryEstimation().estimate(dimensions0, 64).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(
                MemoryRange.of(968),
                WCCType.FORK_JOIN.memoryEstimation().estimate(dimensions100, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(7016),
                WCCType.FORK_JOIN.memoryEstimation().estimate(dimensions100, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(55400),
                WCCType.FORK_JOIN.memoryEstimation().estimate(dimensions100, 64).memoryUsage());

        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
        assertEquals(
                MemoryRange.of(800_122_070_496L).min,
                WCCType.FORK_JOIN.memoryEstimation().estimate(dimensions100B, 1).memoryUsage().min);
        assertEquals(
                MemoryRange.of(6_400_976_563_240L).min,
                WCCType.FORK_JOIN.memoryEstimation().estimate(dimensions100B, 8).memoryUsage().min);
        assertEquals(
                MemoryRange.of(51_207_812_505_192L).min,
                WCCType.FORK_JOIN.memoryEstimation().estimate(dimensions100B, 64).memoryUsage().min);
    }

    @Test
    void memRecFJMerge() {
        GraphDimensions dimensions0 = new GraphDimensions.Builder().setNodeCount(0).build();

        assertEquals(
                MemoryRange.of(168),
                WCCType.FJ_MERGE.memoryEstimation().estimate(dimensions0, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(616),
                WCCType.FJ_MERGE.memoryEstimation().estimate(dimensions0, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(4200),
                WCCType.FJ_MERGE.memoryEstimation().estimate(dimensions0, 64).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(
                MemoryRange.of(968),
                WCCType.FJ_MERGE.memoryEstimation().estimate(dimensions100, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(7016),
                WCCType.FJ_MERGE.memoryEstimation().estimate(dimensions100, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(55400).min,
                WCCType.FJ_MERGE.memoryEstimation().estimate(dimensions100, 64).memoryUsage().min);

        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
        assertEquals(
                MemoryRange.of(800_122_070_496L).min,
                WCCType.FJ_MERGE.memoryEstimation().estimate(dimensions100B, 1).memoryUsage().min);
        assertEquals(
                MemoryRange.of(6_400_976_563_240L).min,
                WCCType.FJ_MERGE.memoryEstimation().estimate(dimensions100B, 8).memoryUsage().min);
        assertEquals(
                MemoryRange.of(51_207_812_505_192L).min,
                WCCType.FJ_MERGE.memoryEstimation().estimate(dimensions100B, 64).memoryUsage().min);
    }

    private void createTestGraph(int... setSizes) {
        try (Transaction tx = db.beginTx()) {
            for (int setSize : setSizes) {
                createLine(db, setSize);
            }
            tx.success();
        }
    }

    private static void createLine(GraphDatabaseService db, int setSize) {
        Node temp = db.createNode();
        for (int i = 1; i < setSize; i++) {
            Node t = db.createNode();
            temp.createRelationshipTo(t, RELATIONSHIP_TYPE);
            temp = t;
        }
    }
}
