/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.impl.infomap;

import java.lang.reflect.Array;
import java.util.function.Consumer;

final class IndexMap<A> {
    public static final int NO_POS = -1;

    public interface PositionMarker<A> {

        int position(A item);

        void setPosition(A item, int position);
    }

    private PositionMarker<A> marker;
    private A[] items;
    private int length;

    IndexMap(final PositionMarker<A> marker, final Class<A> classOfA, final int size) {
        this.marker = marker;
        this.items = newArray(classOfA, size);
        this.length = size;
    }

    @SuppressWarnings("unchecked")
    private static <A> A[] newArray(final Class<A> classOfA, final int size) {
        return (A[]) Array.newInstance(classOfA, size);
    }

    int size() {
        return length;
    }

    void put(final int index, final A item) {
        items[index] = item;
    }

    A get(final int index) {
        A item = items[index];
        int pos;
        while ((pos = marker.position(item)) != NO_POS) {
            item = items[pos];
        }
        return item;
    }

    void remove(int index) {
        int last = --length;
        if (index > last) {
            A item = items[index];
            int pos;
            while ((pos = marker.position(item)) != NO_POS) {
                item = items[index = pos];
            }
        }
        if (index < last) {
            A lastItem = items[last];
            A itemToRemove = items[index];
            marker.setPosition(itemToRemove, index);
            items[index] = lastItem;
            items[last] = itemToRemove;
        }
    }

    void forEach(final Consumer<? super A> block) {
        for (int i = 0; i < length; i++) {
            block.accept(items[i]);
        }
    }

    A[] array() {
        return items;
    }

    void release() {
        marker = null;
        items = null;
        length = 0;
    }
}
