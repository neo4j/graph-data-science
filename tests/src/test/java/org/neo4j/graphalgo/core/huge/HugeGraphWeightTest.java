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
package org.neo4j.graphalgo.core.huge;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class HugeGraphWeightTest {

    private static final int WEIGHT_BATCH_SIZE = PageUtil.pageSizeFor(MemoryUsage.BYTES_OBJECT_REF);
    // make sure that multiple threads are loading the same batch
    private static final int BATCH_SIZE = 100;
    public static final RelationshipType TYPE = RelationshipType.withName("TYPE");

    private GraphDatabaseAPI db;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void teardown() {
        db.shutdown();
    }

    @Test
    void shouldLoadCorrectWeights() {
        mkDb(WEIGHT_BATCH_SIZE * 2, 2);

        Graph graph = loadGraph(db);

        graph.forEachNode((long node) -> {
            graph.forEachOutgoing(node, (src, tgt) -> {
                long weight = (long) graph.weightOf(src, tgt);
                int fakeId = ((int) src << 16) | (int) tgt & 0xFFFF;
                assertEquals(
                        fakeId,
                        weight,
                        "Wrong weight for (" + src + ")->(" + tgt + ")"
                );
                return true;
            });
            return true;
        });
    }

    @Test
    @Timeout(value = 10_000, unit = TimeUnit.MILLISECONDS)
    void shouldLoadMoreWeights() {
        mkDb(WEIGHT_BATCH_SIZE, 4);
        loadGraph(db);
    }

    private void mkDb(int nodes, int relsPerNode) {
        long[] nodeIds = new long[nodes];

        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < nodes; i++) {
                nodeIds[i] = db.createNode().getId();
            }
            int pageSize = PageUtil.pageSizeFor(MemoryUsage.BYTES_OBJECT_REF);
            for (int i = 0; i < nodes; i += pageSize) {
                int max = Math.min(pageSize, nodes - i);
                for (int j = 0; j < max; j++) {
                    long sourceId = nodeIds[i + j];
                    for (int k = 1; k <= relsPerNode; k++) {
                        int targetIndex = j + k;
                        if (targetIndex >= pageSize) {
                            targetIndex = j - k;
                        }
                        long targetId = nodeIds[i + targetIndex];
                        int weight = ((int) sourceId << 16) | (int) targetId & 0xFFFF;
                        Relationship relationship = db
                                .getNodeById(sourceId)
                                .createRelationshipTo(db.getNodeById(targetId), TYPE);
                        relationship.setProperty("weight", weight);
                    }
                }
            }
            tx.success();
        }
    }

    private Graph loadGraph(final GraphDatabaseAPI db) {
        return new GraphLoader(db)
                .withRelationshipProperties(PropertyMapping.of("weight", 0))
                .withDirection(Direction.OUTGOING)
                .withExecutorService(Pools.DEFAULT)
                .withBatchSize(BATCH_SIZE)
                .load(HugeGraphFactory.class);
    }

}
