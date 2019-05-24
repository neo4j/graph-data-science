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
package org.neo4j.graphalgo.core.sources;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.NodeIterator;

import java.util.Arrays;
import java.util.Random;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;

/**
 * NodeIterator adapter with randomized order
 *
 * @author mknblch
 */
public class ShuffledNodeIterator implements NodeIterator {

    private final int nodeCount;

    public ShuffledNodeIterator(int nodeCount) {
        this.nodeCount = nodeCount;
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        final PrimitiveLongIterator nodeIterator = nodeIterator();
        while (nodeIterator.hasNext()) {
            if (!consumer.test(nodeIterator.next())) {
                break;
            }
        }
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return new ShuffledIterAdapter(nodeCount, System.currentTimeMillis());
    }

    public PrimitiveLongIterator nodeIterator(long seed) {
        return new ShuffledIterAdapter(nodeCount, seed);
    }

    private class ShuffledIterAdapter implements PrimitiveLongIterator {

        private final int nodeCount;
        private final long[] nodes;
        private int offset = 0;

        private ShuffledIterAdapter(int nodeCount, long seed) {
            this.nodeCount = nodeCount;
            this.nodes = new long[nodeCount];
            Arrays.setAll(nodes, i -> i);
            shuffle(nodes, nodeCount, new Random(seed));
        }

        @Override
        public boolean hasNext() {
            return offset < nodeCount;
        }

        @Override
        public long next() {
            return nodes[offset++];
        }
    }

    private static void shuffle(long[] data, int length, Random rnd) {
        long t;
        int r;
        for (int i = 0; i < length; i++) {
            r = i + rnd.nextInt(length - i);
            t = data[i];
            data[i] = data[r];
            data[r] = t;
        }
    }
}
