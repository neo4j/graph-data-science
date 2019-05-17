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
package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.core.IntIdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.loading.ReadHelper;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;

import java.util.Arrays;


abstract class VisitRelationship {

    private final IntIdMap idMap;
    private final boolean shouldSort;

    private int[] targets;
    private int length;
    private long prevNode;
    private boolean isSorted;

    int prevTarget;
    int sourceGraphId;

    VisitRelationship(final IntIdMap idMap, final boolean shouldSort) {
        this.idMap = idMap;
        this.shouldSort = shouldSort;
        if (!shouldSort) {
            isSorted = false;
        }
    }

    abstract void visit(RelationshipSelectionCursor cursor);

    final void prepareNextNode(final int sourceGraphId, final int[] targets) {
        this.sourceGraphId = sourceGraphId;
        length = 0;
        prevTarget = -1;
        prevNode = -1L;
        isSorted = shouldSort;
        this.targets = targets;
    }

    final void prepareNextNode(VisitRelationship other) {
        this.sourceGraphId = other.sourceGraphId;
        length = other.length;
        prevTarget = other.prevTarget;
        prevNode = other.prevNode;
        isSorted = other.isSorted;
        targets = other.targets;
    }

    final boolean addNode(final long nodeId) {
        if (nodeId == prevNode) {
            return false;
        }
        final int targetId = idMap.get(nodeId);
        if (targetId == -1) {
            return false;
        }
        if (isSorted && targetId < prevTarget) {
            isSorted = false;
        }
        targets[length++] = targetId;
        prevNode = nodeId;
        prevTarget = targetId;
        return true;
    }

    final int flush() {
        if (shouldSort) {
            Arrays.sort(targets, 0, length);
            length = checkDistinct(targets, length);
        }
        return length;
    }

    static void visitWeight(
            Read readOp,
            CursorFactory cursors,
            int sourceGraphId,
            int targetGraphId,
            WeightMap weights,
            long relationshipId) {

        // TODO: make access to rel properties better
        try (RelationshipScanCursor scanCursor = cursors.allocateRelationshipScanCursor();
             PropertyCursor pc = cursors.allocatePropertyCursor()) {
            readOp.singleRelationship(relationshipId, scanCursor);
            while (scanCursor.next()) {
                scanCursor.properties(pc);
                double weight = ReadHelper.readProperty(pc, weights.propertyId(), weights.defaultValue());
                if (weight != weights.defaultValue()) {
                    long relId = RawValues.combineIntInt(sourceGraphId, targetGraphId);
                    weights.put(relId, weight);
                }
            }
        }
    }

    static void visitUndirectedWeight(
            Read readOp,
            CursorFactory cursors,
            int sourceGraphId,
            int targetGraphId,
            WeightMap weights,
            long relationshipId) {

        // TODO: make access to rel properties better
        try (RelationshipScanCursor scanCursor = cursors.allocateRelationshipScanCursor();
             PropertyCursor pc = cursors.allocatePropertyCursor()) {
            readOp.singleRelationship(relationshipId, scanCursor);
            while (scanCursor.next()) {
                scanCursor.properties(pc);
                double weight = ReadHelper.readProperty(pc, weights.propertyId(), weights.defaultValue());
                if (weight != weights.defaultValue()) {
                    long relId = RawValues.combineIntInt(sourceGraphId, targetGraphId);
                    weights.put(relId, weight);
                    relId = RawValues.combineIntInt(targetGraphId, sourceGraphId);
                    weights.put(relId, weight);
                }
            }
        }
    }

    private static int checkDistinct(final int[] values, final int len) {
        int prev = -1;
        for (int i = 0; i < len; i++) {
            final int value = values[i];
            if (value == prev) {
                return distinct(values, i, len);
            }
            prev = value;
        }
        return len;
    }

    private static int distinct(final int[] values, final int start, final int len) {
        int prev = values[start - 1];
        int write = start;
        for (int i = start + 1; i < len; i++) {
            final int value = values[i];
            if (value > prev) {
                values[write++] = value;
            }
            prev = value;
        }
        return write;
    }
}

final class VisitOutgoingNoWeight extends VisitRelationship {

    VisitOutgoingNoWeight(final IntIdMap idMap, final boolean shouldSort) {
        super(idMap, shouldSort);
    }

    @Override
    void visit(final RelationshipSelectionCursor cursor) {
        addNode(cursor.targetNodeReference());
    }
}

final class VisitIncomingNoWeight extends VisitRelationship {

    VisitIncomingNoWeight(final IntIdMap idMap, final boolean shouldSort) {
        super(idMap, shouldSort);
    }

    @Override
    void visit(final RelationshipSelectionCursor cursor) {
        addNode(cursor.sourceNodeReference());
    }
}

final class VisitOutgoingWithWeight extends VisitRelationship {

    private final Read readOp;
    private final CursorFactory cursors;
    private final WeightMap weights;

    VisitOutgoingWithWeight(
            final Read readOp,
            final CursorFactory cursors,
            final IntIdMap idMap,
            final boolean shouldSort,
            final WeightMap weights) {
        super(idMap, shouldSort);
        this.readOp = readOp;
        this.cursors = cursors;
        this.weights = weights;
    }

    @Override
    void visit(final RelationshipSelectionCursor cursor) {
        if (addNode(cursor.targetNodeReference())) {
            visitWeight(readOp, cursors, sourceGraphId, prevTarget, weights, cursor.relationshipReference());
        }
    }
}

final class VisitIncomingWithWeight extends VisitRelationship {

    private final Read readOp;
    private final CursorFactory cursors;
    private final WeightMap weights;

    VisitIncomingWithWeight(
            final Read readOp,
            final CursorFactory cursors,
            final IntIdMap idMap,
            final boolean shouldSort,
            final WeightMap weights) {
        super(idMap, shouldSort);
        this.readOp = readOp;
        this.cursors = cursors;
        this.weights = weights;
    }

    @Override
    void visit(final RelationshipSelectionCursor cursor) {
        if (addNode(cursor.sourceNodeReference())) {
            visitWeight(readOp, cursors, prevTarget, sourceGraphId, weights, cursor.relationshipReference());
        }
    }
}

final class VisitUndirectedOutgoingWithWeight extends VisitRelationship {

    private final Read readOp;
    private final CursorFactory cursors;
    private final WeightMap weights;

    VisitUndirectedOutgoingWithWeight(
            final Read readOp,
            final CursorFactory cursors,
            final IntIdMap idMap,
            final boolean shouldSort,
            final WeightMap weights) {
        super(idMap, shouldSort);
        this.readOp = readOp;
        this.cursors = cursors;
        this.weights = weights;
    }

    @Override
    void visit(final RelationshipSelectionCursor cursor) {
        if (addNode(cursor.targetNodeReference())) {
            visitUndirectedWeight(readOp, cursors, sourceGraphId, prevTarget, weights, cursor.relationshipReference());
        }
    }
}
