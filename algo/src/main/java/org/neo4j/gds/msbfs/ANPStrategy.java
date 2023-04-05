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
package org.neo4j.gds.msbfs;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.collections.cursor.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

/**
 * "Aggregated Neighbor Processing" (ANP) strategy as described in
 * The More the Merrier: Efficient Multi-Source Graph Traversal
 * http://www.vldb.org/pvldb/vol8/p449-then.pdf
 * <p>
 * The ANP strategy provides two invariants:
 * <ul>
 * <li>
 * For a single thread, a single node is traversed at most once at a given depth
 * – That is, the combination of {@code (nodeId, depth)} appears at most once per thread.
 * It may be that a node is traversed multiple times, but then always at different depths.
 * </li>
 * <li>
 * For multiple threads, the {@code (nodeId, depth)} may appear multiple times,
 * but then always for different sources.
 * </li>
 * </ul>
 */
public class ANPStrategy implements ExecutionStrategy {

    final BfsConsumer perNodeAction;

    public ANPStrategy(BfsConsumer perNodeAction) {
        this.perNodeAction = perNodeAction;
    }

    @Override
    public void run(
        RelationshipIterator relationships,
        long totalNodeCount,
        SourceNodes sourceNodes,
        HugeLongArray visitSet,
        HugeLongArray visitNextSet,
        HugeLongArray seenSet,
        @Nullable HugeLongArray seenNextSet
    ) {
        HugeCursor<long[]> visitCursor = visitSet.newCursor();
        HugeCursor<long[]> nextCursor = visitNextSet.newCursor();

        var depth = 0;

        while (true) {
            visitSet.initCursor(visitCursor);
            while (visitCursor.next()) {
                long[] array = visitCursor.array;
                int offset = visitCursor.offset;
                int limit = visitCursor.limit;
                long base = visitCursor.base;
                for (int i = offset; i < limit; ++i) {
                    if (array[i] != 0L) {
                        prepareNextVisit(relationships, array[i], base + i, visitNextSet, depth);
                    }
                }
            }

            ++depth;

            boolean hasNext = false;
            long next;

            visitNextSet.initCursor(nextCursor);
            while (nextCursor.next()) {
                long[] array = nextCursor.array;
                int offset = nextCursor.offset;
                int limit = nextCursor.limit;
                long base = nextCursor.base;
                for (int i = offset; i < limit; ++i) {
                    if (array[i] != 0L) {
                        next = visitNext(base + i, seenSet, visitNextSet);
                        if (next != 0L) {
                            sourceNodes.reset(next);
                            perNodeAction.accept(base + i, depth, sourceNodes);
                            hasNext = true;
                        }
                    }
                }
            }

            if (stopTraversal(hasNext, depth)) {
                return;
            }

            visitNextSet.copyTo(visitSet, totalNodeCount);
            visitNextSet.fill(0L);
        }
    }

    protected boolean stopTraversal(boolean hasNext, int depth) {
        return !hasNext;
    }

    protected void prepareNextVisit(
        RelationshipIterator relationships,
        long nodeVisit,
        long nodeId,
        HugeLongArray nextSet,
        int depth
    ) {
        relationships.forEachRelationship(
            nodeId,
            (src, tgt) -> {
                nextSet.or(tgt, nodeVisit);
                return true;
            }
        );
    }

    private long visitNext(long nodeId, HugeLongArray seenSet, HugeLongArray nextSet) {
        long seen = seenSet.get(nodeId);
        long next = nextSet.and(nodeId, ~seen);
        seenSet.or(nodeId, next);
        return next;
    }
}
