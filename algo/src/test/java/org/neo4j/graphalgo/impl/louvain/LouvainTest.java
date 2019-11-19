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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.CommunityHelper.assertCommunities;
import static org.neo4j.graphalgo.core.ProcedureConstants.TOLERANCE_DEFAULT;

/**
 * (a)-(b) (d)
 * | /            -> (abc)-(d)
 * (c)
 */
class LouvainTest extends LouvainTestBase {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name:'a', seed1: 0, seed2: 1})" +
        ", (b:Node {name:'b', seed1: 0, seed2: 1})" +
        ", (c:Node {name:'c', seed1: 2, seed2: 1})" +
        ", (d:Node {name:'d', seed1: 2, seed2: 42})" +
        ", (a)-[:TYPE {weight: 1.0}]->(b)" +
        ", (b)-[:TYPE {weight: 1.0}]->(c)" +
        ", (c)-[:TYPE {weight: 5.0}]->(a)" +
        ", (a)-[:TYPE {weight: 1.0}]->(c)";

    @AllGraphTypesTest
    void testWeightedLouvain(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER, true);
        final Louvain louvain = new Louvain(
            graph,
            SINGLE_LEVEL_CONFIG,
            direction,
            Pools.DEFAULT,
            1,
            AllocationTracker.EMPTY
        )
            .withProgressLogger(TestProgressLogger.INSTANCE)
            .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
            .compute();

        final HugeLongArray communityIds = louvain.finalDendrogram();

        assertEquals(3, communityIds.get(0));
        assertCommunities(communityIds, new long[]{0, 1, 2}, new long[]{3});
    }

    @AllGraphTypesTest
    void testLouvain(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);
        final Louvain louvain = new Louvain(graph, DEFAULT_CONFIG, direction, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
            .withProgressLogger(TestProgressLogger.INSTANCE)
            .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
            .compute();
        final HugeLongArray dendogram = louvain.finalDendrogram();
        assertTrue(1 <= louvain.levels());
        assertDefaultCommunityResult(dendogram);
    }

    @AllGraphTypesTest
    void testLouvainWithDendrogram(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);
        final Louvain louvain = new Louvain(
            graph,
            DEFAULT_CONFIG_WITH_DENDROGRAM,
            direction,
            Pools.DEFAULT,
            1,
            AllocationTracker.EMPTY
        )
            .withProgressLogger(TestProgressLogger.INSTANCE)
            .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
            .compute();
        final HugeLongArray[] dendogram = louvain.dendrograms();
        assertEquals(louvain.levels(), dendogram.length);
        assertTrue(1 <= louvain.levels());
        assertDefaultCommunityResult(louvain.finalDendrogram());
    }

    static Stream<Arguments> seededLouvainParameter() {
        Stream<Class<? extends GraphFactory>> graphTypes = Stream.concat(
            TestSupport.allTypesWithoutCypher(),
            TestSupport.cypherType()
        );
        return graphTypes.flatMap(graphType -> Stream.of(
            Arguments.of(graphType, "seed1", new long[]{0, 1, 2, 3}, null),
            Arguments.of(graphType, "seed2", new long[]{0, 1, 2}, new long[]{3})
        ));
    }

    @AllGraphTypesTest
    @MethodSource("seededLouvainParameter")
    void testSeededLouvain(
        Class<? extends GraphFactory> graphImpl,
        String seedProperty,
        long[] community1,
        long[] community2
    ) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER, "seed1", "seed2");
        final Louvain louvain = new Louvain(
            graph,
            new Louvain.Config(
                10,
                10,
                TOLERANCE_DEFAULT,
                false,
                Optional.of(seedProperty)
            ),
            direction,
            Pools.DEFAULT,
            1,
            AllocationTracker.EMPTY
        )
            .withProgressLogger(TestProgressLogger.INSTANCE)
            .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
            .compute();

        if (community2 == null) {
            assertCommunities(louvain.finalDendrogram(), community1);
        } else {
            assertCommunities(louvain.finalDendrogram(), community1, community2);
        }
    }

    @AllGraphTypesTest
    void testSeededLouvainWithoutSeedPropertyAndSparseNodeMapping(Class<? extends GraphFactory> graphImpl) {
        String sparseGraph =
            "CREATE" +
            "  (:Some)" +
            ", (:Other)" +
            ", (:Nodes)" +
            ", (:That)" +
            ", (:We)" +
            ", (:Will)" +
            ", (:Ignore)" +
            ", (:In)" +
            ", (:This)" +
            ", (:Test)" +
            ", (a:Node {name:'a'})" +
            ", (b:Node {name:'b'})" +
            ", (c:Node {name:'c'})" +
            ", (a)-[:TYPE {weight: 1.0}]->(b)" +
            ", (b)-[:TYPE {weight: 1.0}]->(c)" +
            ", (c)-[:TYPE {weight: 1.0}]->(a)" +
            ", (a)-[:TYPE {weight: 1.0}]->(c)";

        Graph graph = loadGraph(graphImpl, sparseGraph);

        Louvain louvain = new Louvain(
            graph,
            DEFAULT_CONFIG,
            direction,
            Pools.DEFAULT,
            1,
            AllocationTracker.EMPTY
        )
            .withProgressLogger(TestProgressLogger.INSTANCE)
            .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
            .compute();

        assertCommunities(louvain.finalDendrogram(), new long[]{0, 1, 2});
    }

    @Test
    void testMemoryEstimationComputation() {
        LouvainFactory factory = new LouvainFactory(DEFAULT_CONFIG);

        GraphDimensions dimensions0 = new GraphDimensions.Builder().setNodeCount(0).build();
        assertEquals(MemoryRange.of(632, 1096), factory.memoryEstimation().estimate(dimensions0, 1).memoryUsage());
        assertEquals(MemoryRange.of(1136, 1600), factory.memoryEstimation().estimate(dimensions0, 4).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(MemoryRange.of(7032, 14696), factory.memoryEstimation().estimate(dimensions100, 1).memoryUsage());
        assertEquals(MemoryRange.of(14736, 22400), factory.memoryEstimation().estimate(dimensions100, 4).memoryUsage());

        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
        assertEquals(
            MemoryRange.of(6400976563256L, 13602075196672L),
            factory.memoryEstimation().estimate(dimensions100B, 1).memoryUsage()
        );
        assertEquals(
                MemoryRange.of(13602075196712L, 20803173830128L),
                factory.memoryEstimation().estimate(dimensions100B, 4).memoryUsage());
    }

    private void assertDefaultCommunityResult(HugeLongArray communities) {
        assertEquals(2, communities.get(0));
        assertCommunities(communities, new long[]{0, 1, 2}, new long[]{3});
    }

}
