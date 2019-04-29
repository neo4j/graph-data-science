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

import com.carrotsearch.hppc.IntLongMap;
import com.carrotsearch.hppc.IntLongScatterMap;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

/**
 * Specialized parallel exporter for temporary louvainGraphs
 *
 * @author mknblch
 */
public class LouvainGraphExporter extends StatementApi{

    private final ExecutorService pool;
    private final int concurrency;

    public LouvainGraphExporter(GraphDatabaseAPI api, ExecutorService pool, int concurrency) {
        super(api);
        this.pool = pool;
        this.concurrency = concurrency;
    }

    public void export(LouvainGraph louvainGraph, String label, String relationship) {

        // number of nodes equals previous community count
        final int nodeCount = Math.toIntExact(louvainGraph.nodeCount());
        // mapping between inner nodeId and neo4j long nodeId for new nodes
        final IntLongMap mapping = new IntLongScatterMap(nodeCount);
        // write jobs
        final ArrayList<Runnable> tasks = new ArrayList<>();
            // label for the augmented graph
            final int labelId = applyInTransaction(statement -> statement.tokenWrite().labelGetOrCreateForName(label));
            // relationship for the augmented graph
            final int relationshipId = applyInTransaction(statement -> statement.tokenWrite().relationshipTypeGetOrCreateForName(relationship));
            // create nodes sequential
            acceptInTransaction(statement -> {
                final Write write = statement.dataWrite();
                for (int i = 0; i < nodeCount; i++) {
                    final long nodeId = write.nodeCreate();
                    mapping.put(i, nodeId);
                    write.nodeAddLabel(nodeId, labelId);
                }
            });
            // partition nodes
            louvainGraph.batchIterables(nodeCount / concurrency)
                    .forEach(it -> tasks.add(() -> process(louvainGraph, it, mapping, relationshipId)));


        ParallelUtil.run(tasks, pool);
    }

    private void process(RelationshipIterator iterator, PrimitiveIntIterable iterable, IntLongMap mapping, int relationshipId) {
            applyInTransaction(statement -> {
                Write write = statement.dataWrite();
                for (PrimitiveIntIterator it = iterable.iterator(); it.hasNext();) {
                    final int next = it.next();
                    iterator.forEachRelationship(next, Direction.OUTGOING, (s, t, r) -> {
                        try {
                            write.relationshipCreate(mapping.get(s), relationshipId, mapping.get(t));
                        } catch (EntityNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                        return true;
                    });
                }
                return null;
            });

    }
}
