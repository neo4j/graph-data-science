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

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.LongSet;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class NodesBatchBuffer extends RecordsBatchBuffer<NodeRecord> {

    public static final int LABEL_NOT_FOUND = -2;
    public static final int EMPTY_LABEL = -1;

    private final LongSet nodeLabelIds;
    private final NodeStore nodeStore;
    private final Map<String, BitSet> labelBitSets;
    private final Map<Long, List<String>> labelMapping;


    // property ids, consecutive
    private final long[] properties;

    public NodesBatchBuffer(
        final NodeStore store,
        final LongSet labels,
        final Map<Long, List<String>> labelMapping,
        int capacity,
        boolean readProperty
    ) {
        super(capacity);
        this.nodeLabelIds = labels;
        this.nodeStore = store;
        this.labelBitSets = nodeLabelIds.isEmpty() ? null : new HashMap<>(labelMapping.keySet().size());
        this.labelMapping = labelMapping;
        this.properties = readProperty ? new long[capacity] : null;
    }

    @Override
    public void offer(final NodeRecord record) {
        long labelId = hasCorrectLabel(record);
        if (labelId != LABEL_NOT_FOUND) {
            add(record.getId(), record.getNextProp(), labelId);
        }
    }

    public void add(long nodeId, long propertiesIndex, long labelId) {
        int len = length++;
        buffer[len] = nodeId;
        if (properties != null) {
            properties[len] = propertiesIndex;
        }

        if (labelBitSets != null && labelId != EMPTY_LABEL) {
            if (labelMapping.containsKey(labelId)) {
                for (String identifier : labelMapping.get(labelId)) {
                    if (!labelBitSets.containsKey(identifier)) {
                        labelBitSets.put(identifier, new BitSet(capacity()));
                    }
                    labelBitSets.get(identifier).set(len);
                }
            }
        }
    }

    // TODO: something label scan store
    private long hasCorrectLabel(final NodeRecord record) {
        if (nodeLabelIds.isEmpty()) {
            return EMPTY_LABEL;
        }
        final long[] labels = NodeLabelsField.get(record, nodeStore);
        for (long l : labels) {
            if (nodeLabelIds.contains(l)) {
                return l;
            }
        }
        return LABEL_NOT_FOUND;
    }

    long[] properties() {
        return this.properties;
    }

    Map<String, BitSet> labelBitSets() {
        return this.labelBitSets;
    }
}
