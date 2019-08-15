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

import org.neo4j.graphalgo.core.huge.loader.AbstractStorePageCacheScanner.RecordConsumer;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public abstract class RecordsBatchBuffer<Record extends AbstractBaseRecord> implements RecordConsumer<Record> {

    // For nodes: the buffer containes the node ids, consecutive
    // For relationships: the buffer is divided into 4-long blocks
    // for each rel: source, target, rel-id, prop-id
    final long[] buffer;
    int length;

    RecordsBatchBuffer(int capacity) {
        this.buffer = new long[capacity];
    }

    boolean scan(AbstractStorePageCacheScanner<Record>.Cursor cursor) {
        reset();
        return cursor.bulkNext(this);
    }

    public int length() {
        return length;
    }

    public int capacity() {
        return buffer.length;
    }

    public boolean isFull() {
        return length >= buffer.length;
    }

    public void reset() {
        this.length = 0;
    }

    long[] batch() {
        return buffer;
    }

}
