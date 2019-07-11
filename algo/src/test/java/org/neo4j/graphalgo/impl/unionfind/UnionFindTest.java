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
package org.neo4j.graphalgo.impl.unionfind;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeNullWeightMap;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.DisjointSetStruct;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class UnionFindTest {

    private static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    private static final int SETS_COUNT = 16;
    private static final int SET_SIZE = 10;

    private static final UnionFindSeq.Config DEFAULT_CONFIG = new UnionFind.Config(
            new HugeNullWeightMap(-1),
            Double.NaN
    );

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "Heavy"},
                new Object[]{HugeGraphFactory.class, "Huge"},
                new Object[]{GraphViewFactory.class, "Kernel"}
        );
    }

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() {
        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println(
                "creating test graph took " + l + " ms"))) {
            int[] setSizes = new int[SETS_COUNT];
            Arrays.fill(setSizes, SET_SIZE);
            createTestGraph(setSizes);
        }
    }

    private Graph graph;

    public UnionFindTest(
            Class<? extends GraphFactory> graphImpl,
            String name) {
        graph = new GraphLoader(DB)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(graphImpl);
    }

    private static void createTestGraph(int... setSizes) {
        DB.executeAndCommit(db -> {
            for (int setSize : setSizes) {
                createLine(db, setSize);
            }
        });
    }

    private static void createLine(GraphDatabaseService db, int setSize) {
        Node temp = db.createNode();
        for (int i = 1; i < setSize; i++) {
            Node t = db.createNode();
            temp.createRelationshipTo(t, RELATIONSHIP_TYPE);
            temp = t;
        }
    }

    @Test
    public void testSeq() {
        test(UnionFindType.SEQ);
    }

    @Test
    public void testQueue() {
        test(UnionFindType.QUEUE);
    }

    @Test
    public void testForkJoin() {
        test(UnionFindType.FORK_JOIN);
    }

    @Test
    public void testFJMerge() {
        test(UnionFindType.FJ_MERGE);
    }

    @Test
    public void memRecSeq() {
        GraphDimensions dimensions0 = new GraphDimensions.Builder().setNodeCount(0).build();
        assertEquals(
                MemoryRange.of(160),
                UnionFindType.SEQ.memoryEstimation().estimate(dimensions0, 1).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(
                MemoryRange.of(1760),
                UnionFindType.SEQ.memoryEstimation().estimate(dimensions100, 1).memoryUsage());

        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
        assertEquals(
                MemoryRange.of(1600244140816L),
                UnionFindType.SEQ.memoryEstimation().estimate(dimensions100B, 1).memoryUsage());
    }

    @Test
    public void memRecQueue() {
        GraphDimensions dimensions0 = new GraphDimensions.Builder().setNodeCount(0).build();

        assertEquals(
                MemoryRange.of(224),
                UnionFindType.QUEUE.memoryEstimation().estimate(dimensions0, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(1008),
                UnionFindType.QUEUE.memoryEstimation().estimate(dimensions0, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(7280),
                UnionFindType.QUEUE.memoryEstimation().estimate(dimensions0, 64).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(
                MemoryRange.of(1824),
                UnionFindType.QUEUE.memoryEstimation().estimate(dimensions100, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(13808),
                UnionFindType.QUEUE.memoryEstimation().estimate(dimensions100, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(109680),
                UnionFindType.QUEUE.memoryEstimation().estimate(dimensions100, 64).memoryUsage());

        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
        assertEquals(
                MemoryRange.of(1600244140880L),
                UnionFindType.QUEUE.memoryEstimation().estimate(dimensions100B, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(12801953126256L),
                UnionFindType.QUEUE.memoryEstimation().estimate(dimensions100B, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(102415625009264L),
                UnionFindType.QUEUE.memoryEstimation().estimate(dimensions100B, 64).memoryUsage());
    }

    @Test
    public void memRecForkJoin() {
        GraphDimensions dimensions0 = new GraphDimensions.Builder().setNodeCount(0).build();

        assertEquals(
                MemoryRange.of(224),
                UnionFindType.FORK_JOIN.memoryEstimation().estimate(dimensions0, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(1008),
                UnionFindType.FORK_JOIN.memoryEstimation().estimate(dimensions0, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(7280),
                UnionFindType.FORK_JOIN.memoryEstimation().estimate(dimensions0, 64).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(
                MemoryRange.of(1824),
                UnionFindType.FORK_JOIN.memoryEstimation().estimate(dimensions100, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(13808),
                UnionFindType.FORK_JOIN.memoryEstimation().estimate(dimensions100, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(109680),
                UnionFindType.FORK_JOIN.memoryEstimation().estimate(dimensions100, 64).memoryUsage());

        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
        assertEquals(
                MemoryRange.of(1600244140880L),
                UnionFindType.FORK_JOIN.memoryEstimation().estimate(dimensions100B, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(12801953126256L),
                UnionFindType.FORK_JOIN.memoryEstimation().estimate(dimensions100B, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(102415625009264L),
                UnionFindType.FORK_JOIN.memoryEstimation().estimate(dimensions100B, 64).memoryUsage());
    }

    @Test
    public void memRecFJMerge() {
        GraphDimensions dimensions0 = new GraphDimensions.Builder().setNodeCount(0).build();

        assertEquals(
                MemoryRange.of(232),
                UnionFindType.FJ_MERGE.memoryEstimation().estimate(dimensions0, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(1016),
                UnionFindType.FJ_MERGE.memoryEstimation().estimate(dimensions0, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(7288),
                UnionFindType.FJ_MERGE.memoryEstimation().estimate(dimensions0, 64).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(
                MemoryRange.of(1832),
                UnionFindType.FJ_MERGE.memoryEstimation().estimate(dimensions100, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(13816),
                UnionFindType.FJ_MERGE.memoryEstimation().estimate(dimensions100, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(109688),
                UnionFindType.FJ_MERGE.memoryEstimation().estimate(dimensions100, 64).memoryUsage());

        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
        assertEquals(
                MemoryRange.of(1600244140888L),
                UnionFindType.FJ_MERGE.memoryEstimation().estimate(dimensions100B, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(12801953126264L),
                UnionFindType.FJ_MERGE.memoryEstimation().estimate(dimensions100B, 8).memoryUsage());
        assertEquals(
                MemoryRange.of(102415625009272L),
                UnionFindType.FJ_MERGE.memoryEstimation().estimate(dimensions100B, 64).memoryUsage());
    }

    private void test(UnionFindType uf) {
        DisjointSetStruct result = run(uf);

        Assert.assertEquals(SETS_COUNT, result.getSetCount());
        long[] setRegions = new long[SETS_COUNT];
        Arrays.fill(setRegions, -1);

        graph.forEachNode((nodeId) -> {
            long expectedSetRegion = nodeId / SET_SIZE;
            final long setId = result.setIdOf(nodeId);
            int setRegion = (int) (setId / SET_SIZE);
            assertEquals(
                    "Node " + nodeId + " in unexpected set: " + setId,
                    expectedSetRegion,
                    setRegion);

            long regionSetId = setRegions[setRegion];
            if (regionSetId == -1) {
                setRegions[setRegion] = setId;
            } else {
                assertEquals(
                        "Inconsistent set for node " + nodeId + ", is " + setId + " but should be " + regionSetId,
                        regionSetId,
                        setId);
            }
            return true;
        });
    }

    private DisjointSetStruct run(final UnionFindType uf) {
        return UnionFindHelper.run(
                uf,
                graph,
                Pools.DEFAULT,
                SET_SIZE / Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT_CONCURRENCY,
                DEFAULT_CONFIG,
                AllocationTracker.EMPTY);
    }
}
