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
package org.neo4j.graphalgo.core.utils.partition;

import org.neo4j.graphalgo.annotation.ValueClass;

import java.util.function.LongConsumer;

@ValueClass
public interface Partition {

    int MAX_NODE_COUNT = (Integer.MAX_VALUE - 32) >> 1;

    long startNode();

    long nodeCount();

    default void consume(LongConsumer consumer) {
        var startNode = startNode();
        long endNode = startNode + nodeCount();
        for (long id = startNode; id < endNode; id++) {
            consumer.accept(id);
        }
    }

    static Partition of(long startNode, long nodeCount) {
        return ImmutablePartition.of(startNode, nodeCount);
    }
}
