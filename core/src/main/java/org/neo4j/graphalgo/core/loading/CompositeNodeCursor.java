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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;

import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.PriorityQueue;

public class CompositeNodeCursor extends DefaultCloseListenable implements NodeLabelIndexCursor {

    private PriorityQueue<NodeLabelIndexCursor> cursorQueue;
    private final List<NodeLabelIndexCursor> cursors;
    private NodeLabelIndexCursor current;
    private final IdentityHashMap<NodeLabelIndexCursor, Integer> cursorLabelIdMapping;

    private boolean closed = false;

    public CompositeNodeCursor(List<NodeLabelIndexCursor> cursors, int[] labelIds) {
        this.cursors = cursors;
        this.cursorQueue = null;
        this.cursorLabelIdMapping = new IdentityHashMap<>();

        for (int i = 0; i < cursors.size(); i++) {
            cursorLabelIdMapping.put(cursors.get(i), labelIds[i]);
        }
    }

    NodeLabelIndexCursor getCursor(int index) {
        return cursors.get(index);
    }

    int currentLabel() {
        return this.cursorLabelIdMapping.get(current);
    }

    @Override
    public LabelSet labels() {
        return current.labels();
    }

    @Override
    public void node(NodeCursor cursor) {
        current.node(cursor);
    }

    @Override
    public long nodeReference() {
        return current.nodeReference();
    }

    @Override
    public float score() {
        return current.score();
    }

    @Override
    public boolean next() {
        if (cursorQueue == null) {
            cursorQueue = new PriorityQueue<>(Comparator.comparingLong(NodeIndexCursor::nodeReference));
            cursors.forEach(cursor -> {
                if (cursor.next()) {
                    cursorQueue.add(cursor);
                }
            });
        }
        if (current != null && current.next()) {
            cursorQueue.add(current);
        }

        if (cursorQueue.isEmpty()) {
            current = null;
            return false;
        }
        else {
            current = cursorQueue.poll();
            return true;
        }
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {
        cursors.forEach(cursor -> cursor.setTracer(tracer));
    }

    @Override
    public void removeTracer() {
        cursors.forEach(Cursor::removeTracer);
    }

    @Override
    public void close() {
        this.closed = true;
        closeInternal();

        if (this.closeListener != null) {
            this.closeListener.onClosed(this);
        }
    }

    @Override
    public void closeInternal() {
        cursors.forEach(Cursor::closeInternal);
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }
}
