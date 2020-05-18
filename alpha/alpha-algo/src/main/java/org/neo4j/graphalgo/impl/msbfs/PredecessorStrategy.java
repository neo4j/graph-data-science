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
package org.neo4j.graphalgo.impl.msbfs;

import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Non-optimized execution MSBFS strategy as described in
 * The More the Merrier: Efficient Multi-Source Graph Traversal
 * http://www.vldb.org/pvldb/vol8/p449-then.pdf
 * <p>
 * This strategy allows accessing the predecessor node at each BFS-level.
 */
public class PredecessorStrategy implements MultiSourceBFS.ExecutionStrategy {

    private final BfsWithPredecessorConsumer perNodeAction;

    PredecessorStrategy(BfsWithPredecessorConsumer perNodeAction) {
        this.perNodeAction = perNodeAction;
    }

    @Override
    public void run(
        RelationshipIterator relationships,
        long totalNodeCount,
        MultiSourceBFS.SourceNodes sourceNodes,
        HugeLongArray visitSet,
        HugeLongArray nextSet,
        HugeLongArray seenSet
    ) {
        var visitCursor = visitSet.newCursor();
        var depth = new AtomicInteger(0);
        var hasNext = new AtomicBoolean(false);

        while (true) {
            hasNext.set(false);
            depth.incrementAndGet();

            visitSet.initCursor(visitCursor);
            while (visitCursor.next()) {
                long[] array = visitCursor.array;
                int offset = visitCursor.offset;
                int limit = visitCursor.limit;
                long base = visitCursor.base;

                for (int i = offset; i < limit; ++i) {
                    long nodeId = base + i;
                    long visit = array[i];
                    if (visit != 0L) {
                        relationships.forEachRelationship(nodeId, (source, target) -> {
                            // D ← visit[nodeId] & ∼seen[target]
                            long next = visitSet.get(nodeId) & ~seenSet.get(target);

                            if (next != 0L) {
                                // visitNext[target] ← visitNext[target] | D
                                nextSet.or(target, next);

                                // seen[target] ← seen[target] | D
                                //
                                // If we set seen[target] at this point, target
                                // won't be visited again on the same level by
                                // the same BFS. Since we want to trigger the
                                // node action for all predecessors ending up in
                                // target, we need to defer setting seen.
                                seenSet.or(nodeId, next);

                                // do BFS computation on target
                                sourceNodes.reset(next);
                                perNodeAction.accept(target, nodeId, depth.get(), sourceNodes);
                                hasNext.set(true);
                            }

                            return true;
                        });
                    }
                }

                if (!hasNext.get()) {
                    return;
                }

                nextSet.copyTo(visitSet, totalNodeCount);
                nextSet.fill(0L);
            }
        }
    }
}
