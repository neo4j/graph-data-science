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
package org.neo4j.gds.core.huge;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipCursor;
import org.neo4j.gds.api.RelationshipIterator;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

public class AllRelationshipsSpliterator implements Spliterator<RelationshipCursor> {

    private final RelationshipIterator relationshipIterator;
    private final double fallbackValue;

    // state
    private long current;
    private long limit;
    private Iterator<RelationshipCursor> cursorIterator;

    public AllRelationshipsSpliterator(Graph graph, double fallbackValue) {
        this(graph, 0, graph.nodeCount(), fallbackValue);
    }

    private AllRelationshipsSpliterator(
        RelationshipIterator relationshipIterator,
        long fromNode,
        long toNodeExclusive,
        double fallbackValue
    ) {
        this.relationshipIterator = relationshipIterator.concurrentCopy();
        this.current = fromNode;
        this.limit = toNodeExclusive;
        this.fallbackValue = fallbackValue;
        this.cursorIterator = this.relationshipIterator.streamRelationships(this.current, fallbackValue).iterator();
    }

    @Override
    public boolean tryAdvance(Consumer<? super RelationshipCursor> action) {
        boolean isAdvanced = advance(action);

        if (!isAdvanced && hasRemaining()) {
            this.current++;
            this.cursorIterator = relationshipIterator.streamRelationships(current, this.fallbackValue).iterator();
            isAdvanced = advance(action);
        }

        return isAdvanced || hasRemaining();
    }

    private boolean advance(Consumer<? super RelationshipCursor> action) {
        if (this.cursorIterator.hasNext()) {
            action.accept(this.cursorIterator.next());
            return true;
        }
        return false;
    }

    private boolean hasRemaining() {
        return this.current < this.limit - 1;
    }

    @Override
    public Spliterator<RelationshipCursor> trySplit() {
        long remaining = this.limit - this.current;
        if (remaining < 2) {
            return null;
        }

        long split = this.current + remaining / 2;
        long newLimit = this.limit;
        this.limit = split;

        return new AllRelationshipsSpliterator(this.relationshipIterator, split, newLimit, this.fallbackValue);
    }

    @Override
    public long estimateSize() {
        return this.limit - this.current;
    }

    @Override
    public int characteristics() {
        return Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.SIZED;
    }
}
