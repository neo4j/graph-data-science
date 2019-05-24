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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

/**
 * Helper methods for reading degrees of nodes.
 * This is in addition to {@link org.neo4j.internal.kernel.api.helpers.Nodes} with added
 * methods for counting undirected nodes.
 * <p>
 * Importing {@code undirected} is different from importing {@code both}:
 * {@code both} internally imports {@code outgoing} and {@code incoming} separately and nodes that loop
 * to themselves will be imported accordingly; relationships will be flattened into a single iteration during
 * iteration at query-time. In contrast, {@code undirected} merges all relationships on-the-fly and collapses
 * them into a single relationship storage during load-time. Nodes that loop to themselves will be
 * removed (de-duplicated) eventually, but have to be buffered during import first. We can only drop
 * consecutive loops, but this is not guaranteed to always be the case – loops can also be non-consecutive
 * and for those we need the upper bound during import buffering.
 * <p>
 * The existing methods
 * {@link org.neo4j.internal.kernel.api.helpers.Nodes#countAll(NodeCursor, CursorFactory)} and
 * {@link org.neo4j.internal.kernel.api.helpers.Nodes#countAll(NodeCursor, CursorFactory, int)}
 * return a degree count that, while being technically correct, does not cover the full buffer-size that we require.
 */
public final class NodesHelper {

    /**
     * Counts all the relationships from node where the cursor is positioned.
     * <p>
     * NOTE: Is different from {@link org.neo4j.internal.kernel.api.helpers.Nodes#countAll(NodeCursor, CursorFactory)}]
     * in that it counts loops twice. See the class-level docs for the reasoning.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param cursors    a factory for cursors
     * @return the number of relationships from the node
     */
    public static int countUndirected(NodeCursor nodeCursor, CursorFactory cursors) {
        if (nodeCursor.isDense()) {
            try (RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor()) {
                nodeCursor.relationships(group);
                int count = 0;
                while (group.next()) {
                    count += group.outgoingCount() + group.incomingCount() + (group.loopCount() << 1);
                }
                return count;
            }
        } else {
            try (RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor()) {
                int count = 0;
                nodeCursor.allRelationships(traversal);
                long nodeId = nodeCursor.nodeReference();
                while (traversal.next()) {
                    ++count;
                    // loop has self node as source and target, count it twice
                    if (traversal.sourceNodeReference() == nodeId && traversal.targetNodeReference() == nodeId) {
                        ++count;
                    }
                }
                return count;
            }
        }
    }

    /**
     * Counts all the relationships of the given type from node where the cursor is positioned.
     * <p>
     * NOTE: Is different from {@link org.neo4j.internal.kernel.api.helpers.Nodes#countAll(NodeCursor, CursorFactory, int)}
     * in that it counts loops twice. See the class-level docs for the reasoning.
     *
     * @param nodeCursor a cursor positioned at the node whose relationships we're counting
     * @param cursors    a factory for cursors
     * @param type       the type of the relationship we're counting
     * @return the number relationships from the node with the given type
     */
    public static int countUndirected(NodeCursor nodeCursor, CursorFactory cursors, int type) {
        if (nodeCursor.isDense()) {
            try (RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor()) {
                nodeCursor.relationships(group);
                int count = 0;
                while (group.next()) {
                    if (group.type() == type) {
                        return group.outgoingCount() + group.incomingCount() + (group.loopCount() << 1);
                    }
                }
                return count;
            }
        } else {
            try (RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor()) {
                int count = 0;
                nodeCursor.allRelationships(traversal);
                long nodeId = nodeCursor.nodeReference();
                while (traversal.next()) {
                    if (traversal.type() == type) {
                        count++;
                        // loop has self node as source and target, count it twice
                        if (traversal.sourceNodeReference() == nodeId && traversal.targetNodeReference() == nodeId) {
                            ++count;
                        }
                    }
                }
                return count;
            }
        }
    }

    private NodesHelper() {
        throw new UnsupportedOperationException("Do not instantiate");
    }
}
