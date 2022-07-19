/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.core.huge;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.PageUtil;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.getNodeById;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runInTransaction;

final class HugeGraphWeightTest extends BaseTest {

    private static final RelationshipType TYPE = RelationshipType.withName("TYPE");

    @Test
    void shouldLoadCorrectWeights() {
        int nodeCount = PageUtil.pageSizeFor(PageUtil.PAGE_SIZE_32KB, MemoryUsage.BYTES_OBJECT_REF) * 2;
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
        int nodeCount = PageUtil.pageSizeFor(PageUtil.PAGE_SIZE_32KB, MemoryUsage.BYTES_OBJECT_REF);
        mkDb(nodeCount, 4);
        loadGraph(db);
    }

    private void mkDb(int nodes, int relsPerNode) {
        long[] nodeIds = new long[nodes];

        runInTransaction(db, tx -> {
            for (int i = 0; i < nodes; i++) {
                nodeIds[i] = tx.createNode().getId();
            }
            int pageSize = PageUtil.pageSizeFor(PageUtil.PAGE_SIZE_32KB, MemoryUsage.BYTES_OBJECT_REF);
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
                        Relationship relationship = getNodeById(tx, sourceId)
                            .createRelationshipTo(getNodeById(tx, targetId), TYPE);
                        relationship.setProperty("weight", propertyValue);
                    }
                }
            }
        });
    }

    private Graph loadGraph(final GraphDatabaseService db) {
        return new StoreLoaderBuilder()
            .databaseService(db)
            .addRelationshipProperty(PropertyMapping.of("weight", 0))
            .build()
            .graph();
    }

}
