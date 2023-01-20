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
package org.neo4j.gds.collections;

import java.util.stream.Stream;

/**
 * A common super interface for non-primitive huge sparse array lists.
 * It should be used for convenience reasons only, as using this interface
 * as a reference type will result in dynamic dispatch of its methods
 * that cannot be inlined by C2.
 * It is therefore recommended to reference the individual implementations
 * directly for best performance.
 */
public interface HugeSparseObjectArrayList<OBJ, CONSUMER> {

    /**
     * @return the current maximum number of values that can be stored in the list
     */
    long capacity();

    /**
     * @return true, iff the value at the given index is not the default value
     */
    boolean contains(long index);

    /**
     * @return the object array at the given index
     */
    OBJ get(long index);

    /**
     * Sets the value at the given index.
     */
    void set(long index, OBJ value);

    /**
     * Applies to given consumer to all non-default values stored in the list.
     */
    void forAll(CONSUMER consumer);

    /**
     * Returns an iterator that consumes the underlying pages of this list.
     * Once the iterator has been consumed, the list is empty and will return
     * the default value for each index.
     */
    DrainingIterator<OBJ[]> drainingIterator();

    /**
     * Returns a stream of the underlying data.
     * The stream will skip over null pages and will otherwise stream over
     * the full page, potentially containing default values.
     */
    Stream<OBJ> stream();
}
