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
package org.neo4j.gds.core.utils.partition;

import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;

public class Partition {

    private final long startNode;
    private final long nodeCount;

    static final int MAX_NODE_COUNT = (Integer.MAX_VALUE - 32) >> 1;

    public Partition(long startNode, long nodeCount) {
        this.startNode = startNode;
        this.nodeCount = nodeCount;
    }

    public long startNode() {
        return startNode;
    }

    public long nodeCount() {
        return nodeCount;
    }

    public void consume(LongConsumer consumer) {
        var startNode = startNode();
        long endNode = startNode + nodeCount();
        for (long id = startNode; id < endNode; id++) {
            consumer.accept(id);
        }
    }

    public static Partition of(long startNode, long nodeCount) {
        return new Partition(startNode, nodeCount);
    }

    public LongStream stream() {
        var start = startNode();
        return LongStream.range(start, start + nodeCount());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Partition partition = (Partition) o;
        return startNode == partition.startNode && nodeCount == partition.nodeCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startNode, nodeCount);
    }
}
