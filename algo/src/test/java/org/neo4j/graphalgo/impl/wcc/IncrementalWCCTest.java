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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IncrementalWCCTest extends WCCBaseTest {

    private static final String SEED_PROPERTY = "community";

    private static final int COMMUNITY_COUNT = 16;
    private static final int COMMUNITY_SIZE = 10;

    /**
     * Create multiple communities and connect them pairwise.
     */
    @BeforeAll
    static void setupGraphDb() {
        DB = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = DB.beginTx()) {
            for (int i = 0; i < COMMUNITY_COUNT; i = i + 2) {
                long community1 = createLineGraph(DB);
                long community2 = createLineGraph(DB);
                createConnection(DB, community1, community2);
            }
            tx.success();
        }
    }

    private static long getCommunityId(long nodeId, int communitySize) {
        return nodeId / communitySize;
    }

    private static void createConnection(GraphDatabaseService db, long sourceId, long targetId) {
        final Node source = db.getNodeById(sourceId);
        final Node target = db.getNodeById(targetId);

        source.createRelationshipTo(target, RELATIONSHIP_TYPE);
    }

    @ParameterizedTest(name = "{0} -- {1}")
    @MethodSource("parameters")
    void shouldComputeComponentsFromSeedProperty(Class<? extends GraphFactory> graphFactory, WCCType unionFindType) {
        Graph graph = new GraphLoader(DB)
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
            long expectedCommunityId = getCommunityId(nodeId, 2 * COMMUNITY_SIZE) * 2;
            long actualCommunityId = result.setIdOf(nodeId);
            assertEquals(
                    expectedCommunityId,
                    actualCommunityId,
                    "Node " + nodeId + " in unexpected set: " + actualCommunityId
            );
            return true;
        });
    }

    @Override
    int communitySize() {
        return COMMUNITY_SIZE;
    }

    /**
     * Creates a line graph of the given length (i.e. numer of relationships).
     *
     * @param db database
     * @return the last node id inserted into the graph
     */
    static long createLineGraph(GraphDatabaseService db) {
        Node temp = db.createNode();
        long communityId = getCommunityId(temp.getId(), COMMUNITY_SIZE);

        for (int i = 1; i < COMMUNITY_SIZE; i++) {
            temp.setProperty(SEED_PROPERTY, communityId);
            Node target = db.createNode();
            temp.createRelationshipTo(target, RELATIONSHIP_TYPE);
            temp = target;
        }
        temp.setProperty(SEED_PROPERTY, communityId);
        return temp.getId();
    }

}
