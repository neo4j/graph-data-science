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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Non-optimized execution MSBFS strategy as described in
 * The More the Merrier: Efficient Multi-Source Graph Traversal
 * <a href="http://www.vldb.org/pvldb/vol8/p449-then.pdf">http://www.vldb.org/pvldb/vol8/p449-then.pdf</a>
 * <p>
 * This strategy allows accessing the predecessor node at each BFS-level.
 */
public class PredecessorStrategy implements ExecutionStrategy {

    private final BfsConsumer perNodeAction;
    private final BfsWithPredecessorConsumer perNeighborAction;

    PredecessorStrategy(BfsConsumer perNodeAction, BfsWithPredecessorConsumer perNeighborAction) {
        this.perNodeAction = perNodeAction;
        this.perNeighborAction = perNeighborAction;
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
        Objects.requireNonNull(seenNextSet, "seenNextSet must always be initialized with the PredecessorStrategy");
        try (
            var visitCursor = visitSet.newCursor();
            var seenCursor = seenSet.newCursor();
            var seenNextCursor = seenNextSet.newCursor()) {

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
                            // User-defined computation on source.
                            // Happens exactly once for each node.
                            sourceNodes.reset(visit);
                            perNodeAction.accept(nodeId, depth.get() - 1, sourceNodes);

                            relationships.forEachRelationship(nodeId, (source, target) -> {
                                // D ← visit[nodeId] & ∼seen[target]
                                long next = visitSet.get(nodeId) & ~seenSet.get(target);

                                if (next != 0L) {
                                    // visitNext[target] ← visitNext[target] | D
                                    visitNextSet.or(target, next);

                                    // seen[target] ← seen[target] | D
                                    seenNextSet.or(target, next);

                                    // User-defined computation on source and target.
                                    // Happens as often as the target is discovered
                                    // per BFS-level from different source nodes.
                                    sourceNodes.reset(next);
                                    perNeighborAction.accept(target, nodeId, depth.get(), sourceNodes);
                                    hasNext.set(true);
                                }

                                return true;
                            });
                        }
                    }

                    if (!hasNext.get()) {
                        return;
                    }

                    // Update seen set with seen nodes from current level
                    HugeCursor<long[]> seen = seenSet.initCursor(seenCursor);
                    HugeCursor<long[]> seenNext = seenNextSet.initCursor(seenNextCursor);
                    updateSeenSet(seen, seenNext);

                    // Prepare visit set for next level
                    visitNextSet.copyTo(visitSet, totalNodeCount);
                    visitNextSet.fill(0L);
                }
            }
        }
    }

    private void updateSeenSet(HugeCursor<long[]> seen, HugeCursor<long[]> seenNext) {
        while (seen.next()) {
            seenNext.next();
            long[] seenArray = seen.array;
            long[] seenNextArray = seenNext.array;
            int end = seen.limit;
            int pos = seen.offset;
            for (; pos < end - 4; pos += 4) {
                seenArray[pos] |= seenNextArray[pos];
                seenArray[pos + 1] |= seenNextArray[pos + 1];
                seenArray[pos + 2] |= seenNextArray[pos + 2];
                seenArray[pos + 3] |= seenNextArray[pos + 3];
            }
            for (; pos < end; pos++) {
                seenArray[pos] |= seenNextArray[pos];
            }
        }
    }
}
