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

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;

public final class HugeGraphWeightTest {

    private static final int WEIGHT_BATCH_SIZE = PageUtil.pageSizeFor(MemoryUsage.BYTES_OBJECT_REF);
    // make sure that multiple threads are loading the same batch
    private static final int BATCH_SIZE = 100;

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldLoadCorrectWeights() throws Exception {
        mkDb(WEIGHT_BATCH_SIZE << 1, 2);
        Graph graph = loadGraph(db);

        graph.forEachNode((long node) -> {
            graph.forEachOutgoing(node, (src, tgt) -> {
                long weight = (long) graph.weightOf(src, tgt);
                int fakeId = ((int) src << 16) | (int) tgt & 0xFFFF;
                assertEquals(
                        "Wrong weight for (" + src + ")->(" + tgt + ")",
                        fakeId, weight);
                return true;
            });
            return true;
        });
    }

    @Test(timeout = 10000)
    public void shouldLoadMoreWeights() throws Exception {
        mkDb(WEIGHT_BATCH_SIZE, 4);
        loadGraph(db);
    }

    private void mkDb(final int nodes, final int relsPerNode) {
        db.executeAndCommit((GraphDatabaseService __) -> {
            try (KernelTransaction st = db.transaction()) {
                TokenWrite token = st.tokenWrite();
                int type = token.relationshipTypeGetOrCreateForName("TYPE");
                int key = token.propertyKeyGetOrCreateForName("weight");
                Write write = st.dataWrite();
                NewRel newRel = newRel(write, type, key);

                long[] nodeIds = new long[nodes];
                for (int i = 0; i < nodes; i++) {
                    nodeIds[i] = write.nodeCreate();
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
                            newRel.mk(sourceId, targetId);
                        }
                    }
                }
            } catch (KernelException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Graph loadGraph(final GraphDatabaseAPI db) {
        return new GraphLoader(db)
                .withRelationshipWeightsFromProperty("weight", 0)
                .withDirection(Direction.OUTGOING)
                .withExecutorService(Pools.DEFAULT)
                .withBatchSize(BATCH_SIZE)
                .load(HugeGraphFactory.class);
    }

    private interface NewRel {
        void mk(long source, long target) throws KernelException;
    }

    private static NewRel newRel(
            Write write,
            int type,
            int key) {
        return (source, target) -> {
            int fakeId = ((int) source << 16) | (int) target & 0xFFFF;
            long rel = write.relationshipCreate(source, type, target);
            write.relationshipSetProperty(rel, key, Values.intValue(fakeId));
        };
    }
}
