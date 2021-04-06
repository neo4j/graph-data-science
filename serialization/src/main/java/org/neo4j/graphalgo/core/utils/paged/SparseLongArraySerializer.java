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
package org.neo4j.graphalgo.core.utils.paged;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SparseLongArraySerializer extends Serializer<SparseLongArray> {

    @Override
    public void write(
        Kryo kryo, Output output, SparseLongArray sparseLongArray
    ) {
        kryo.writeObject(output, sparseLongArray.highestNeoId());
        kryo.writeObject(output, sparseLongArray.idCount());
        kryo.writeObject(output, sparseLongArray.array());
        kryo.writeObject(output, sparseLongArray.blockMapping());
        kryo.writeObject(output, sparseLongArray.blockOffsets());
        kryo.writeObject(output, sparseLongArray.sortedBlockOffsets());
    }

    @Override
    public SparseLongArray read(
        Kryo kryo, Input input, Class<? extends SparseLongArray> type
    ) {
        long highestNeoId = kryo.readObject(input, Long.class);
        long idCount = kryo.readObject(input, Long.class);
        long[] array = kryo.readObject(input, long[].class);
        int[] blockMapping = kryo.readObject(input, int[].class);
        long[] blockOffsets = kryo.readObject(input, long[].class);
        long[] sortedBlockOffsets = kryo.readObject(input, long[].class);
        return new SparseLongArray(
            idCount,
            highestNeoId,
            array,
            blockOffsets,
            sortedBlockOffsets,
            blockMapping
        );
    }
}
