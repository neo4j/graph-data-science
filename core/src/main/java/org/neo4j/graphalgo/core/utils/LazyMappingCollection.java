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

import com.carrotsearch.hppc.AbstractIterator;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;

public final class LazyMappingCollection<T, U> extends AbstractCollection<U> {

    private final Collection<T> base;
    private final Function<T, U> mappingFunction;

    public static <T, U> Collection<U> of(
            final Collection<T> base,
            final Function<T, U> mappingFunction) {
        return new LazyMappingCollection<>(base, mappingFunction);
    }

    private LazyMappingCollection(
            final Collection<T> base,
            final Function<T, U> mappingFunction) {
        this.base = base;
        this.mappingFunction = mappingFunction;
    }

    @Override
    public Iterator<U> iterator() {
        return new AbstractIterator<U>() {
            private final Iterator<T> it = base.iterator();

            @Override
            protected U fetch() {
                if (it.hasNext()) {
                    return mappingFunction.apply(it.next());
                }
                return done();
            }
        };
    }

    @Override
    public int size() {
        return base.size();
    }
}
