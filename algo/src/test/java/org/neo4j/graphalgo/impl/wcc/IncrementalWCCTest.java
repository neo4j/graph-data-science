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

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.lang.System.lineSeparator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;

class IncrementalWCCTest {

    private static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");
    private static final String SEED_PROPERTY = "community";

    private static final int COMMUNITY_COUNT = 16;
    private static final int COMMUNITY_SIZE = 10;

    static Stream<Arguments> parameters() {
        return TestSupport.allTypesWithoutCypher().
            flatMap(graphType -> Arrays.stream(WCCType.values())
                .map(ufType -> Arguments.of(graphType, ufType)));
    }

    private GraphDatabaseAPI db;

    /**
     * Create multiple communities and connect them pairwise.
     */
    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < COMMUNITY_COUNT; i = i + 2) {
                long community1 = createLineGraph(db);
                long community2 = createLineGraph(db);
                createConnection(db, community1, community2);
            }
            tx.success();
        }
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @ParameterizedTest(name = "{0} -- {1}")
    @MethodSource("parameters")
    void shouldComputeComponentsFromSeedProperty(Class<? extends GraphFactory> graphFactory, WCCType unionFindType) {
        Graph graph = new GraphLoader(db)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withOptionalNodeProperties(PropertyMapping.of(SEED_PROPERTY, SEED_PROPERTY, -1L))
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(graphFactory);

        WCC.Config config = new WCC.Config(
                graph.nodeProperties(SEED_PROPERTY),
                Double.NaN
        );

        // We expect that UF connects pairs of communites
        DisjointSetStruct result = run(unionFindType, graph, config);
        assertEquals(COMMUNITY_COUNT / 2, getSetCount(result));

        graph.forEachNode((nodeId) -> {
            // Since the community size has doubled, the community id first needs to be computed with twice the
            // community size. To account for the gaps in the community ids when communities have merged,
            // we need to multiply the resulting id by two.
            long expectedCommunityId = nodeId / (2 * COMMUNITY_SIZE) * 2;
            long actualCommunityId = result.setIdOf(nodeId);
            assertEquals(
                    expectedCommunityId,
                    actualCommunityId,
                    "Node " + nodeId + " in unexpected set: " + actualCommunityId
            );
            return true;
        });
    }

    @ParameterizedTest(name = "WCCType = {0}")
    @EnumSource(WCCType.class)
    void shouldAssignMinimumCommunityIdOnMerge(WCCType wccType) {
        // Given
        // Simulates new node (a) created with lower ID and no seed
        Graph graph = fromGdl(
            "  (a {id: 1, seed: 43})" +
            ", (b {id: 2, seed: 42})" +
            ", (c {id: 3, seed: 42})" +
            ", (a)-->(b)" +
            ", (b)-->(c)"
        );

        // When
        WCC.Config config = new WCC.Config(
            graph.nodeProperties("seed"),
            Double.NaN
        );

        DisjointSetStruct result = WCCHelper.run(
            wccType,
            graph,
            ParallelUtil.DEFAULT_BATCH_SIZE,
            Pools.DEFAULT_CONCURRENCY,
            config
        );

        String actual = resultString(graph, result);

        // Then
        String expected = String.format(
            "0, 42%n" +
            "1, 42%n" +
            "2, 42"
        );

        assertEquals(expected, actual);
    }

    private void createConnection(GraphDatabaseService db, long sourceId, long targetId) {
        final Node source = db.getNodeById(sourceId);
        final Node target = db.getNodeById(targetId);

        source.createRelationshipTo(target, RELATIONSHIP_TYPE);
    }

    /**
     * Creates a line graph of the given length (i.e. numer of relationships).
     *
     * @param db database
     * @return the last node id inserted into the graph
     */
    private long createLineGraph(GraphDatabaseService db) {
        Node temp = db.createNode();
        long communityId = temp.getId() / COMMUNITY_SIZE;

        for (int i = 1; i < COMMUNITY_SIZE; i++) {
            temp.setProperty(SEED_PROPERTY, communityId);
            Node target = db.createNode();
            temp.createRelationshipTo(target, RELATIONSHIP_TYPE);
            temp = target;
        }
        temp.setProperty(SEED_PROPERTY, communityId);
        return temp.getId();
    }

    private DisjointSetStruct run(WCCType uf, Graph graph, WCC.Config config) {
        return WCCHelper.run(
            uf,
            graph,
            COMMUNITY_SIZE / Pools.DEFAULT_CONCURRENCY,
            Pools.DEFAULT_CONCURRENCY,
            config
        );
    }

    /**
     * Compute number of sets present.
     */
    private long getSetCount(DisjointSetStruct struct) {
        long capacity = struct.size();
        BitSet sets = new BitSet(capacity);
        for (long i = 0L; i < capacity; i++) {
            long setId = struct.setIdOf(i);
            sets.set(setId);
        }
        return sets.cardinality();
    }

    private String resultString(IdMapping idMapping, DisjointSetStruct dss) {
        return LongStream.range(IdMapping.START_NODE_ID, idMapping.nodeCount())
            .mapToObj(mappedId -> String.format("%d, %d",
                idMapping.toOriginalNodeId(mappedId),
                dss.setIdOf(mappedId))
            ).collect(Collectors.joining(lineSeparator()));
    }
}
