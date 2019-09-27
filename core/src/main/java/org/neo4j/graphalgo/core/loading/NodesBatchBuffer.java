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

import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;

public final class NodesBatchBuffer extends RecordsBatchBuffer<NodeRecord> {

    private final int label;
    private final NodeStore nodeStore;

    // property ids, consecutive
    private final long[] properties;

    public NodesBatchBuffer(final NodeStore store, final int label, int capacity, boolean readProperty) {
        super(capacity);
        this.label = label;
        this.nodeStore = store;
        this.properties = readProperty ? new long[capacity] : null;
    }

    @Override
    public void offer(final NodeRecord record) {
        if (hasCorrectLabel(record)) {
            add(record.getId(), record.getNextProp());
        }
    }

    public void add(long nodeId, long propertiesIndex) {
        int len = length++;
        buffer[len] = nodeId;
        if (properties != null) {
            properties[len] = propertiesIndex;
        }
    }

    // TODO: something label scan store
    private boolean hasCorrectLabel(final NodeRecord record) {
        if (label == Read.ANY_LABEL) {
            return true;
        }
        final long[] labels = NodeLabelsField.get(record, nodeStore);
        long label = (long) this.label;
        for (long l : labels) {
            if (l == label) {
                return true;
            }
        }
        return false;
    }

    long[] properties() {
        return properties;
    }
}
