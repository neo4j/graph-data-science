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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.huge.loader.AbstractStorePageCacheScanner.RecordConsumer;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;


final class RelationshipsBatchBuffer implements RecordConsumer<RelationshipRecord> {

    private final IdMapping idMap;
    private final int type;

    // 4-long blocks for each rel
    // source, target, rel-id, prop-id
    private final long[] buffer;
    private final long[] sortCopy;
    private final int[] histogram;

    private int length;


    RelationshipsBatchBuffer(final IdMapping idMap, final int type, int capacity) {
        this.idMap = idMap;
        this.type = type;
        int bufferLength = Math.multiplyExact(4, capacity);
        buffer = new long[bufferLength];
        sortCopy = RadixSort.newCopy(buffer);
        histogram = RadixSort.newHistogram(capacity);
    }

    boolean scan(AbstractStorePageCacheScanner<RelationshipRecord>.Cursor cursor) {
        length = 0;
        return cursor.bulkNext(this) && length > 0;
    }

    @Override
    public void add(final RelationshipRecord record) {
        if ((type & record.getType()) == record.getType()) {
            long source = idMap.toMappedNodeId(record.getFirstNode());
            if (source != -1L) {
                long target = idMap.toMappedNodeId(record.getSecondNode());
                if (target != -1L) {
                    int position = this.length;
                    long[] buffer = this.buffer;
                    buffer[position] = source;
                    buffer[1 + position] = target;
                    buffer[2 + position] = record.getId();
                    buffer[3 + position] = record.getNextProp();
                    this.length = 4 + position;
                }
            }
        }
    }

    long[] sortBySource() {
        RadixSort.radixSort(buffer, sortCopy, histogram, length);
        return buffer;
    }

    long[] sortByTarget() {
        RadixSort.radixSort2(buffer, sortCopy, histogram, length);
        return buffer;
    }

    int length() {
        return length;
    }

    long[] spareLongs() {
        return sortCopy;
    }

    int[] spareInts() {
        return histogram;
    }
}
