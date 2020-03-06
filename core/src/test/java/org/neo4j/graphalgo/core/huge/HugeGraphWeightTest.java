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
package org.neo4j.graphalgo.core.huge;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.NativeFactory;
import org.neo4j.graphalgo.compat.GraphDbApi;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.createNode;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.getNodeById;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.runInTransaction;

final class HugeGraphWeightTest {

    public static final RelationshipType TYPE = RelationshipType.withName("TYPE");

    private GraphDbApi db;

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
        int nodeCount = PageUtil.pageSizeFor(MemoryUsage.BYTES_OBJECT_REF) * 2;
        mkDb(nodeCount, 2);

        Graph graph = loadGraph(db);

        graph.forEachNode((long node) -> {
            graph.forEachRelationship(node, (src, tgt) -> {
                long propertyValue = (long) graph.relationshipProperty(src, tgt, Double.NaN);
                int fakeId = ((int) src << 16) | (int) tgt & 0xFFFF;
                assertEquals(
                        fakeId,
                        propertyValue,
                        "Wrong propertyValue for (" + src + ")->(" + tgt + ")"
                );
                return true;
            });
            return true;
        });
    }

    @Test
    @Timeout(value = 10_000, unit = TimeUnit.MILLISECONDS)
    void shouldLoadMoreWeights() {
        int nodeCount = PageUtil.pageSizeFor(MemoryUsage.BYTES_OBJECT_REF);
        mkDb(nodeCount, 4);
        loadGraph(db);
    }

    private void mkDb(int nodes, int relsPerNode) {
        long[] nodeIds = new long[nodes];

        runInTransaction(db, tx -> {
            for (int i = 0; i < nodes; i++) {
                nodeIds[i] = createNode(db, tx).getId();
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
                        int propertyValue = ((int) sourceId << 16) | (int) targetId & 0xFFFF;
                        Relationship relationship = getNodeById(db, tx, sourceId)
                            .createRelationshipTo(getNodeById(db, tx, targetId), TYPE);
                        relationship.setProperty("weight", propertyValue);
                    }
                }
            }
        });
    }

    private Graph loadGraph(final GraphDatabaseAPI db) {
        return new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .addRelationshipProperty(PropertyMapping.of("weight", 0))
            .build()
            .load(NativeFactory.class);
    }

}
