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
package org.neo4j.gds.beta.pregel;

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.PrimitiveIterator;

public final class Messages implements Iterable<Double> {

    @NotNull
    @Override
    public Iterator<Double> iterator() {
        return iterator;
    }

    public interface MessageIterator extends PrimitiveIterator.OfDouble {
        boolean isEmpty();
    }

    private final MessageIterator iterator;

    Messages(MessageIterator iterator) {
        this.iterator = iterator;
    }

    @NotNull
    public PrimitiveIterator.OfDouble doubleIterator() {
        return iterator;
    }

    public boolean isEmpty() {
        return iterator.isEmpty();
    }
}
