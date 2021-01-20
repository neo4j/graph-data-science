/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.ml;

import org.neo4j.gds.ml.nodemodels.LongArrayAccessor;

import java.util.Iterator;
import java.util.function.Function;

public class MappedBatch implements Batch {
    private final Batch delegate;
    private final Function<Long, Long> fn;

    public MappedBatch(Batch delegate, Function<Long, Long> fn) {
        this.delegate = delegate;
        this.fn = fn;
    }

    @Override
    public Iterable<Long> nodeIds() {
        if (fn == LongArrayAccessor.IDENTITY) {
            return delegate.nodeIds();
        }
        return () -> {
            var originalIterator = delegate.nodeIds().iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return originalIterator.hasNext();
                }

                @Override
                public Long next() {
                    return fn.apply(originalIterator.next());
                }
            };
        };
    }

    @Override
    public int size() {
        return delegate.size();
    }
}
