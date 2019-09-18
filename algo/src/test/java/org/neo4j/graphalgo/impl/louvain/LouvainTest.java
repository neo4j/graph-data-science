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
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.CypherGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * (a)-(b)-(d)
 * | /            -> (abc)-(d)
 * (c)
 */
class LouvainTest extends LouvainTestBase {

    private static final String DB_CYPHER = "CREATE" +
                                            "  (a:Node {name:'a'})" +
                                            ", (b:Node {name:'b'})" +
                                            ", (c:Node {name:'c'})" +
                                            ", (d:Node {name:'d'})" +
                                            ", (a)-[:TYPE {weight: 1.0}]->(b)" +
                                            ", (b)-[:TYPE {weight: 1.0}]->(c)" +
                                            ", (c)-[:TYPE {weight: 1.0}]->(a)" +
                                            ", (a)-[:TYPE {weight: 1.0}]->(c)";

    private static final Label LABEL = Label.label("Node");
    private static final String ABCD = "abcd";

    @Override
    void setupGraphDb(Graph graph) {
        try (Transaction transaction = DB.beginTx()) {
            for (int i = 0; i < ABCD.length(); i++) {
                final String value = String.valueOf(ABCD.charAt(i));
                final long id = graph.toMappedNodeId(DB.findNode(LABEL, "name", value).getId());
                nameMap.put(value, (int) id);
            }
            transaction.success();
        }
    }

    @AllGraphTypesTest
    void testRunner(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);
        final Louvain algorithm = new Louvain(graph, DEFAULT_CONFIG, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute();
        final HugeLongArray[] dendogram = algorithm.getDendrogram();
        for (int i = 1; i <= dendogram.length; i++) {
            if (null == dendogram[i - 1]) {
                break;
            }
        }
        assertCommunities(algorithm);
    }

    @AllGraphTypesTest
    void testRandomNeighborLouvain(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);
        final Louvain algorithm = new Louvain(graph, DEFAULT_CONFIG, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute();
        final HugeLongArray[] dendogram = algorithm.getDendrogram();
        for (int i = 1; i <= dendogram.length; i++) {
            if (null == dendogram[i - 1]) {
                break;
            }
        }
        assertCommunities(algorithm);
    }

    @AllGraphTypesTest
    void testMultithreadedLouvain(Class<? extends GraphFactory> graphImpl) {
        GraphBuilder.create(DB)
                .setLabel("Node")
                .setRelationship("REL")
                .newCompleteGraphBuilder()
                .createCompleteGraph(200, 1.0);
        GraphLoader graphLoader = new GraphLoader(DB);
        if (graphImpl == CypherGraphFactory.class) {
            graphLoader
                    .withNodeStatement("MATCH (u:Node) RETURN id(u) as id")
                    .withRelationshipStatement("MATCH (u1:Node)-[rel:REL]-(u2:Node) \n" +
                                               "RETURN id(u1) AS source, id(u2) AS target");
        } else {
            graphLoader
                    .withLabel("Node")
                    .withRelationshipType("REL");
        }
        Graph graph = graphLoader
                .withOptionalRelationshipWeightsFromProperty(null, 1.0)
                .withoutNodeWeights()
                .sorted()
                .undirected()
                .load(graphImpl);

        Louvain.Config config = new Louvain.Config(99, 99999);
        Louvain algorithm = new Louvain(graph, config, Pools.DEFAULT, 4, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute(99, 99999);

        assertEquals(1, algorithm.getLevel());
        long[] expected = new long[200];
        HugeLongArray[] dendrogram = algorithm.getDendrogram();
        for (HugeLongArray communities : dendrogram) {
            long[] communityIds = communities.toArray();
            assertArrayEquals(expected, communityIds);
        }
    }

    @Test
    void testMemoryEstimationComputation() {
        LouvainFactory factory = new LouvainFactory(DEFAULT_CONFIG);

        GraphDimensions dimensions0 = new GraphDimensions.Builder().setNodeCount(0).build();
        assertEquals(MemoryRange.of(608, 1072), factory.memoryEstimation().estimate(dimensions0, 1).memoryUsage());
        assertEquals(MemoryRange.of(1112, 1576), factory.memoryEstimation().estimate(dimensions0, 4).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(MemoryRange.of(7008, 14672), factory.memoryEstimation().estimate(dimensions100, 1).memoryUsage());
        assertEquals(MemoryRange.of(14712, 22376), factory.memoryEstimation().estimate(dimensions100, 4).memoryUsage());

        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
        assertEquals(MemoryRange.of(6400976563232L, 13602075196648L), factory.memoryEstimation().estimate(dimensions100B, 1).memoryUsage());
        assertEquals(MemoryRange.of(13602075196688L, 20803173830104L), factory.memoryEstimation().estimate(dimensions100B, 4).memoryUsage());
    }

    private void assertCommunities(Louvain louvain) {
        assertUnion(new String[]{"a", "b", "c"}, louvain.getCommunityIds());
        assertDisjoint(new String[]{"a", "d"}, louvain.getCommunityIds());
    }

}
