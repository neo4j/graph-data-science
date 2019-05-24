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

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;

import static org.neo4j.graphalgo.core.huge.loader.ZigZagLongDecoding.zigZagUncompress;
import static org.neo4j.graphalgo.core.huge.loader.VarLongEncoding.encodeVLongs;
import static org.neo4j.graphalgo.core.huge.loader.VarLongEncoding.encodedVLongSize;
import static org.neo4j.graphalgo.core.huge.loader.VarLongEncoding.zigZag;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfByteArray;

final class CompressedLongArray {

    private static final byte[] EMPTY_BYTES = new byte[0];

    private final AllocationTracker tracker;
    private byte[] storage;
    private int pos;
    private long lastValue;
    private int length;

    CompressedLongArray(AllocationTracker tracker, int length) {
        this.tracker = tracker;
        if (length == Integer.MAX_VALUE) {
            length = 0;
        }
        if (length > 0) {
            tracker.add(sizeOfByteArray(length));
            storage = new byte[length];
        } else {
            storage = EMPTY_BYTES;
        }
    }

    void addDeltas(long[] deltas, int start, int end) {
        long last = lastValue, val;
        int required = 0;
        for (int i = start; i < end; i++) {
            val = zigZag(deltas[i] - last);
            last = deltas[i];
            deltas[i] = val;
            required += encodedVLongSize(val);
        }
        ensureCapacity(pos, required, storage);
        pos = encodeVLongs(deltas, start, end, storage, pos);
        lastValue = last;
        length += (end - start);
    }

    private void ensureCapacity(int pos, int required, byte[] storage) {
        if (storage.length <= pos + required) {
            int newLength = ArrayUtil.oversize(pos + required, Byte.BYTES);
            tracker.remove(sizeOfByteArray(storage.length));
            tracker.add(sizeOfByteArray(newLength));
            this.storage = Arrays.copyOf(storage, newLength);
        }
    }

    int length() {
        return length;
    }

    int uncompress(long[] into) {
        assert into.length >= length;
        return zigZagUncompress(storage, pos, into);
    }

    byte[] internalStorage() {
        return storage;
    }

    void release() {
        if (storage.length > 0) {
            tracker.remove(sizeOfByteArray(storage.length));
        }
        storage = null;
        pos = 0;
        length = 0;
    }
}
