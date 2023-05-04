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
package org.neo4j.gds.core.utils.partition;

import com.carrotsearch.hppc.LongArrayList;
import org.jetbrains.annotations.TestOnly;

import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

public class IteratorPartition {

    private final PrimitiveIterator.OfLong iterator;
    private final long length;

    public IteratorPartition(
        PrimitiveIterator.OfLong iterator,
        long length
    ) {
        this.iterator = iterator;
        this.length = length;
    }

    public long length() {
        return length;
    }

    public void consume(LongConsumer consumer) {
        for (long i = 0; i < length; i++) {
            consumer.accept(iterator.nextLong());
        }
    }

    @Override
    public String toString() {
        return "IteratorPartition{" +
            ", length=" + length +
            '}';
    }

    @TestOnly
    public long[] materialize() {
        var values = new LongArrayList();

        consume(values::add);
        return values.toArray();
    }
}
