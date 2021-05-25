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
package org.neo4j.graphalgo.compat;

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.jetbrains.annotations.Nullable;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;

import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.PriorityQueue;

public abstract class CompositeNodeCursor extends DefaultCloseListenable implements Cursor {

    private final PriorityQueue<NodeLabelIndexCursor> cursorQueue;
    private boolean repopulateCursorQueue;
    private final List<NodeLabelIndexCursor> cursors;
    private NodeLabelIndexCursor current;
    private final LongArrayList currentLabels;
    private final IdentityHashMap<NodeLabelIndexCursor, Integer> cursorLabelIdMapping;

    private boolean closed = false;

    protected CompositeNodeCursor(List<NodeLabelIndexCursor> cursors, int[] labelIds) {
        this.cursors = cursors;
        this.cursorQueue = new PriorityQueue<>(
            cursors.size(),
            Comparator.comparingLong(NodeIndexCursor::nodeReference)
        );
        this.repopulateCursorQueue = true;
        this.cursorLabelIdMapping = new IdentityHashMap<>();
        this.currentLabels = new LongArrayList();

        for (int i = 0; i < cursors.size(); i++) {
            cursorLabelIdMapping.put(cursors.get(i), labelIds[i]);
        }
    }

    @Nullable
    public NodeLabelIndexCursor getCursor(int index) {
        return cursors.get(index);
    }

    public void removeCursor(int index) {
        var cursor = cursors.get(index);
        if (cursor != null) {
            cursor.close();
            cursors.set(index, null);
        }
    }

    public long[] currentLabels() {
        return this.currentLabels.toArray();
    }

    public long nodeReference() {
        return current.nodeReference();
    }

    @Override
    public boolean next() {
        if (repopulateCursorQueue) {
            repopulateCursorQueue = false;
            cursors.forEach(cursor -> {
                if (cursor != null && cursor.next()) {
                    cursorQueue.add(cursor);
                }
            });
        }

        if (current != null && current.next()) {
            cursorQueue.add(current);
        }

        if (cursorQueue.isEmpty()) {
            current = null;
            repopulateCursorQueue = true;
            return false;
        } else {
            current = cursorQueue.poll();

            currentLabels.clear();
            currentLabels.add(cursorLabelIdMapping.get(current));

            NodeLabelIndexCursor next = cursorQueue.peek();
            while (next != null && next.nodeReference() == current.nodeReference()) {
                cursorQueue.poll();
                currentLabels.add(cursorLabelIdMapping.get(next));
                if (next.next()) {
                    cursorQueue.add(next);
                }

                next = cursorQueue.peek();
            }
            return true;
        }
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {
        cursors.forEach(cursor -> {
            if (cursor != null) {
                cursor.setTracer(tracer);
            }
        });
    }

    @Override
    public void removeTracer() {
        cursors.forEach(cursor -> {
            if (cursor != null) {
                cursor.removeTracer();
            }
        });
    }

    public void closeCursor() {
        if (this.isClosed()) {
            return;
        }

        this.closed = true;
        closeInternal();

        if (this.closeListener != null) {
            this.closeListener.onClosed(this);
        }
    }

    @Override
    public void closeInternal() {
        cursors.forEach(cursor -> {
            if (cursor != null) {
                cursor.closeInternal();
            }
        });
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }
}
