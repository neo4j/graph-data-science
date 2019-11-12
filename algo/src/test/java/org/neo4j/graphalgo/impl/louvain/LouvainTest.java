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
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.impl.louvain.legacy.Louvain;
import org.neo4j.graphalgo.impl.louvain.legacy.LouvainFactory;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            ", (b)-[:TYPE {weight: 5.0}]->(c)" +
            ", (c)-[:TYPE {weight: 1.0}]->(a)" +
            ", (a)-[:TYPE {weight: 1.0}]->(c)";

    private static final Label LABEL = Label.label("Node");
    private static final String ABCD = "abcd";

    @Override
    void setupGraphDb(Graph graph) {
        try (Transaction transaction = db.beginTx()) {
            for (int i = 0; i < ABCD.length(); i++) {
                final String value = String.valueOf(ABCD.charAt(i));
                Node namedNode = db.findNode(LABEL, "name", value);
                if (namedNode != null) {
                    long id = graph.toMappedNodeId(namedNode.getId());
                    nameMap.put(value, (int) id);
                }
            }
            transaction.success();
        }
    }

    @AllGraphTypesTest
    void testWeightedLouvain(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);
        final Louvain algorithm = new Louvain(graph, new Louvain.Config(1, 1, Optional.empty()), Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute();

        final HugeLongArray communityIds = algorithm.getCommunityIds();
        assertUnion(new String[]{"b", "c"}, communityIds);
        assertDisjoint(new String[]{"a", "b", "d"}, communityIds);
    }

    @AllGraphTypesTest
    void testLouvain(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);
        final Louvain algorithm = new Louvain(graph, DEFAULT_CONFIG, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute();
        final HugeLongArray[] dendogram = algorithm.getDendrogram();
        assertEquals(0, dendogram.length);
        assertTrue(1 <= algorithm.getLevel());
        assertCommunities(algorithm);
    }

    @AllGraphTypesTest
    void testLouvainWithDendrogram(Class<? extends GraphFactory> graphImpl) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER);
        final Louvain algorithm = new Louvain(graph, DEFAULT_CONFIG_WITH_DENDROGRAM, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute();
        final HugeLongArray[] dendogram = algorithm.getDendrogram();
        assertEquals(algorithm.getLevel(), dendogram.length);
        assertTrue(1 <= algorithm.getLevel());
        assertCommunities(algorithm);
    }

    static Stream<Arguments> seededLouvainParameter() {
        Stream<Class<? extends GraphFactory>> graphTypes = Stream.concat(
                TestSupport.allTypesWithoutCypher(),
                TestSupport.cypherType());
        return graphTypes.flatMap(graphType -> Stream.of(
                Arguments.of(graphType, "seed1", new String[]{"a", "b", "c", "d"}, new String[]{}),
                Arguments.of(graphType, "seed2", new String[]{"a", "b", "c"}, new String[]{"a", "d"})
        ));
    }

    @AllGraphTypesTest
    @MethodSource("seededLouvainParameter")
    void testSeededLouvain(
            Class<? extends GraphFactory> graphImpl,
            String seedProperty,
            String[] expectedUnion,
            String[] expectedDisjoint) {
        Graph graph = loadGraph(graphImpl, DB_CYPHER, "seed1", "seed2");
        NodeProperties communityMap = graph.nodeProperties(seedProperty);
        final Louvain algorithm = new Louvain(
                graph,
                DEFAULT_CONFIG,
                communityMap,
                Pools.DEFAULT,
                1,
                AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute();
        assertUnion(expectedUnion, algorithm.getCommunityIds());
        if (expectedDisjoint.length > 0) {
            assertDisjoint(expectedDisjoint, algorithm.getCommunityIds());
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

        Louvain algorithm = new Louvain(
            graph,
            DEFAULT_CONFIG,
            null,
            Pools.DEFAULT,
            1,
            AllocationTracker.EMPTY
        )
            .withProgressLogger(TestProgressLogger.INSTANCE)
            .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
            .compute();
        assertUnion(new String[]{"a", "b", "c"}, algorithm.getCommunityIds());
    }

    @AllGraphTypesTest
    void testMultithreadedLouvain(Class<? extends GraphFactory> graphImpl) {
        GraphBuilder.create(db)
                .setLabel("Node")
                .setRelationship("REL")
                .newCompleteGraphBuilder()
                .createCompleteGraph(200, 1.0);
        GraphLoader graphLoader = new GraphLoader(db);
        if (graphImpl == CypherGraphFactory.class) {
            graphLoader
                    .withNodeStatement("MATCH (u:Node) RETURN id(u) AS id")
                    .withRelationshipStatement("MATCH (u1:Node)-[rel:REL]-(u2:Node) \n" +
                                               "RETURN id(u1) AS source, id(u2) AS target");
        } else {
            graphLoader
                    .withLabel("Node")
                    .withRelationshipType("REL");
        }
        Graph graph;
        try (Transaction tx = db.beginTx()) {
            graph = graphLoader
                    .withRelationshipProperties(PropertyMapping.of(null, 1.0))
                    .sorted()
                    .undirected()
                    .load(graphImpl);
        }

        Louvain.Config config = new Louvain.Config(99, 99999, Optional.empty());
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
        assertEquals(MemoryRange.of(632, 1096), factory.memoryEstimation().estimate(dimensions0, 1).memoryUsage());
        assertEquals(MemoryRange.of(1136, 1600), factory.memoryEstimation().estimate(dimensions0, 4).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(MemoryRange.of(7032, 14696), factory.memoryEstimation().estimate(dimensions100, 1).memoryUsage());
        assertEquals(MemoryRange.of(14736, 22400), factory.memoryEstimation().estimate(dimensions100, 4).memoryUsage());

        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
        assertEquals(
                MemoryRange.of(6400976563256L, 13602075196672L),
                factory.memoryEstimation().estimate(dimensions100B, 1).memoryUsage());
        assertEquals(
                MemoryRange.of(13602075196712L, 20803173830128L),
                factory.memoryEstimation().estimate(dimensions100B, 4).memoryUsage());
    }

    private void assertCommunities(Louvain louvain) {
        assertUnion(new String[]{"a", "b", "c"}, louvain.getCommunityIds());
        assertDisjoint(new String[]{"a", "d"}, louvain.getCommunityIds());
    }

}
