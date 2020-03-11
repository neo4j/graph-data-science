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

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.graphalgo.compat.StatementConstantsProxy.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.graphalgo.utils.ExceptionUtil.validateNodeIsLoaded;


public final class RelationshipsBatchBuffer extends RecordsBatchBuffer<RelationshipRecord> {

    public static final int RELATIONSHIP_REFERENCE_OFFSET = 2;
    public static final int PROPERTIES_REFERENCE_OFFSET = 3;
    public static final int BATCH_ENTRY_SIZE = 4;
    public static final int BATCH_ENTRY_SHIFT_SIZE = Integer.numberOfTrailingZeros(BATCH_ENTRY_SIZE);

    private final IdMapping idMap;
    private final int type;
    private boolean throwOnUnMappedNodeIds;

    private final long[] sortCopy;
    private final int[] histogram;

    public RelationshipsBatchBuffer(
        final IdMapping idMap,
        final int type,
        int capacity
    ) {
        this(idMap, type, capacity, true);
    }

    public RelationshipsBatchBuffer(
        final IdMapping idMap,
        final int type,
        int capacity,
        boolean throwOnUnMappedNodeIds
    ) {
        // For relationships: the buffer is divided into 4-long blocks
        // for each rel: source, target, rel-id, prop-id
        super(Math.multiplyExact(4, capacity));
        this.idMap = idMap;
        this.type = type;
        this.throwOnUnMappedNodeIds = throwOnUnMappedNodeIds;
        sortCopy = RadixSort.newCopy(buffer);
        histogram = RadixSort.newHistogram(capacity);
    }

    @Override
    public void offer(final RelationshipRecord record) {
        if ((type == ANY_RELATIONSHIP_TYPE) || (type == record.getType())) {
            long source = idMap.toMappedNodeId(record.getFirstNode());
            if (source != -1L) {
                long target = idMap.toMappedNodeId(record.getSecondNode());
                if (target != -1L) {
                    add(source, target, record.getId(), record.getNextProp());
                }
                else if (throwOnUnMappedNodeIds) {
                    validateNodeIsLoaded(source, record.getSecondNode(), "target");
                }
            }
            else if (throwOnUnMappedNodeIds){
                validateNodeIsLoaded(source, record.getFirstNode(), "source");
            }
        }
    }

    public void add(long sourceId, long targetId, long relationshipReference) {
        int position = this.length;
        long[] buffer = this.buffer;
        buffer[position] = sourceId;
        buffer[1 + position] = targetId;
        buffer[2 + position] = relationshipReference;
        this.length = 4 + position;
    }

    public void add(long sourceId, long targetId, long relationshipReference, long propertyReference) {
        int position = this.length;
        long[] buffer = this.buffer;
        buffer[position] = sourceId;
        buffer[1 + position] = targetId;
        buffer[2 + position] = relationshipReference;
        buffer[3 + position] = propertyReference;
        this.length = 4 + position;
    }

    long[] sortBySource() {
        RadixSort.radixSort(buffer, sortCopy, histogram, length);
        return buffer;
    }

    long[] sortByTarget() {
        RadixSort.radixSort2(buffer, sortCopy, histogram, length);
        return buffer;
    }

    long[] spareLongs() {
        return sortCopy;
    }

    int[] spareInts() {
        return histogram;
    }
}
