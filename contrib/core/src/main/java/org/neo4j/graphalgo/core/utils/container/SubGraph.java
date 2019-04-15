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
package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.LongArrayDeque;
import com.carrotsearch.hppc.predicates.LongPredicate;
import org.neo4j.graphalgo.api.AllRelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.RawValues;


/**
 * @author mknblch
 */
public class SubGraph implements AllRelationshipIterator {

    private final LongArrayDeque deque;

    public SubGraph(int expectedElements) {
        deque = new LongArrayDeque(expectedElements);
    }

    public void add(long combinedSourceTarget) {
        deque.addLast(combinedSourceTarget);
    }

    public void add(int sourceNodeId, int targetNodeId) {
        deque.addLast(RawValues.combineIntInt(sourceNodeId, targetNodeId));
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        forEachRelationship((source, target, relationId) -> {
            builder.append(source).append("->").append(target).append("\n");
            return true;
        });
        return builder.toString();
    }

    @Override
    public void forEachRelationship(RelationshipConsumer consumer) {
        deque.forEach((LongPredicate) value ->
                consumer.accept(RawValues.getHead(value), RawValues.getTail(value), -1L));
    }
}
