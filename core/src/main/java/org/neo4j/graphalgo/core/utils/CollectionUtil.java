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

package org.neo4j.graphalgo.core.utils;

import org.eclipse.collections.api.tuple.primitive.IntObjectPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;

import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class CollectionUtil {

    public static <T> Stream<IntObjectPair<T>> enumerate(Collection<T> items) {
        int size = items.size();
        if (items instanceof List && items instanceof RandomAccess) {
            List<T> list = (List<T>) items;
            return IntStream
                .range(0, size)
                .mapToObj(idx -> PrimitiveTuples.pair(idx, list.get(idx)));
        }
        AtomicInteger index = new AtomicInteger();
        return items.stream().map(item -> PrimitiveTuples.pair(index.getAndIncrement(), item));
    }

    private CollectionUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
