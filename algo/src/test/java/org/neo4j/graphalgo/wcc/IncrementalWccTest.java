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
package org.neo4j.graphalgo.wcc;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.StoreConfigBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.ImmutableModernGraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.logging.NullLog;

import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;

class IncrementalWccTest extends AlgoTestBase {

    private static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");
    private static final String SEED_PROPERTY = "community";

    private static final int COMMUNITY_COUNT = 16;
    private static final int COMMUNITY_SIZE = 10;

    /**
     * Create multiple communities and connect them pairwise.
     */
    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
        runInTransaction(db, () -> {
            for (int i = 0; i < COMMUNITY_COUNT; i = i + 2) {
                long community1 = createLineGraph(db);
                long community2 = createLineGraph(db);
                createConnection(db, community1, community2);
            }
        });
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    private static void createConnection(GraphDatabaseService db, long sourceId, long targetId) {
        final Node source = db.getNodeById(sourceId);
        final Node target = db.getNodeById(targetId);

        source.createRelationshipTo(target, RELATIONSHIP_TYPE);
    }

    @Test
    void shouldComputeComponentsFromSeedProperty() {
        Graph graph = ImmutableModernGraphLoader.builder()
            .api(db)
            .log(NullLog.getInstance())
            .createConfig(new StoreConfigBuilder()
                .loadAnyLabel(true)
                .addRelationshipType(RELATIONSHIP_TYPE.name())
                .addNodeProperty(PropertyMapping.of(SEED_PROPERTY, SEED_PROPERTY, -1L))
                .build()
            )
            .legacyMode(false)
            .build()
            .load(HugeGraphFactory.class);

        WccStreamConfig config = ImmutableWccStreamConfig.builder()
            .concurrency(Pools.DEFAULT_CONCURRENCY)
            .seedProperty(SEED_PROPERTY)
            .threshold(0D)
            .build();

        // We expect that UF connects pairs of communites
        DisjointSetStruct result = run(graph, config);
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

    @Test
    void shouldAssignMinimumCommunityIdOnMerge() {
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
        WccStreamConfig config = ImmutableWccStreamConfig.builder()
            .seedProperty("seed")
            .build();

        DisjointSetStruct result = run(graph, config);

        // Then
        LongStream.range(IdMapping.START_NODE_ID, graph.nodeCount())
            .forEach(node -> assertEquals(42, result.setIdOf(node)));
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

    private DisjointSetStruct run(Graph graph, WccBaseConfig config) {
        return new Wcc(
            graph,
            Pools.DEFAULT,
            COMMUNITY_SIZE / Pools.DEFAULT_CONCURRENCY,
            config,
            AllocationTracker.EMPTY
        ).compute();
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

}
