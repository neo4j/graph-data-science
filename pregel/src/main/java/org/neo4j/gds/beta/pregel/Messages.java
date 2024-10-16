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
import java.util.OptionalLong;
import java.util.PrimitiveIterator;

public final class Messages implements Iterable<Double> {

    @NotNull
    @Override
    public Iterator<Double> iterator() {
        return iterator;
    }

    public interface MessageIterator extends PrimitiveIterator.OfDouble {

        boolean isEmpty();

        default OptionalLong sender() {
            return OptionalLong.empty();
        }
    }

    private final MessageIterator iterator;

    Messages(MessageIterator iterator) {
        this.iterator = iterator;
    }

    /**
     * Returns a iterator that can be used to iterate over the messages.
     */
    @NotNull
    public PrimitiveIterator.OfDouble doubleIterator() {
        return this.iterator;
    }

    /**
     * Indicates if there are messages present.
     */
    public boolean isEmpty() {
        return this.iterator.isEmpty();
    }

    /**
     * If the computation defined a {@link org.neo4j.gds.beta.pregel.Reducer}, this method will
     * return the sender of the aggregated message. Depending on the reducer implementation, the
     * sender is deterministically defined by the reducer, e.g., for Max or Min. In any other case,
     * the sender will be one of the node ids that sent messages to that node.
     * <p>
     * Note, that {@link PregelConfig#trackSender()} must return true to enable sender tracking.
     *
     * @return the sender of an aggregated message or an empty optional if no reducer is defined
     */
    public OptionalLong sender() {
        return this.iterator.sender();
    }
}
