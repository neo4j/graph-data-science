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
package org.neo4j.gds.result;

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.procedures.LongLongProcedure;
import org.neo4j.gds.collections.hsa.HugeSparseLongArray;

import java.util.function.LongUnaryOperator;

class CommunityAddTask implements Runnable {

    private final HugeSparseLongArray.Builder builder;

    private final LongUnaryOperator communityFunction;

    private final long startId;
    private final long length;

    // Use local buffer to avoid contention on GrowingBuilder.add().
    // This is especially useful, if the input has a skewed
    // distribution, i.e. most nodes end up in the same community.
    private final LongLongMap buffer;

    CommunityAddTask(
        HugeSparseLongArray.Builder builder,
        LongUnaryOperator communityFunction,
        long startId,
        long length
    ) {
        this.builder = builder;
        this.communityFunction = communityFunction;
        this.startId = startId;
        this.length = length;
        // safe cast, since max batch size less than Integer.MAX_VALUE
        this.buffer = new LongLongHashMap((int) length);
    }

    @Override
    public void run() {
        var endId = startId + length;
        for (long id = startId; id < endId; id++) {
            buffer.addTo(communityFunction.applyAsLong(id), 1L);
        }
        buffer.forEach((LongLongProcedure) builder::addTo);
    }
}
