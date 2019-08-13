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
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;


public final class RelationshipsBatchBuffer extends RecordsBatchBuffer<RelationshipRecord> {

    private final IdMapping idMap;
    private final int type;

    private final long[] sortCopy;
    private final int[] histogram;

    RelationshipsBatchBuffer(final IdMapping idMap, final int type, int capacity) {
        super(Math.multiplyExact(4, capacity));
        this.idMap = idMap;
        this.type = type;
        sortCopy = RadixSort.newCopy(buffer);
        histogram = RadixSort.newHistogram(capacity);
    }

    @Override
    public void add(final RelationshipRecord record) {
        if (type == StatementConstants.ANY_RELATIONSHIP_TYPE || type == record.getType()) {
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

    long[] spareLongs() {
        return sortCopy;
    }

    int[] spareInts() {
        return histogram;
    }
}
