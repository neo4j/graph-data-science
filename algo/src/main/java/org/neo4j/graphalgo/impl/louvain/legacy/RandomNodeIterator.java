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
package org.neo4j.graphalgo.impl.louvain.legacy;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.core.utils.RandomLongIterator;

import java.util.Random;
import java.util.function.LongPredicate;

/**
 * NodeIterator adapter with randomized order
 */
public class RandomNodeIterator implements NodeIterator {

    private final long nodeCount;

    public RandomNodeIterator(long nodeCount) {
        this.nodeCount = nodeCount;
    }

    @Override
    public void forEachNode(final LongPredicate consumer) {
        final PrimitiveLongIterator nodeIterator = nodeIterator();
        while (nodeIterator.hasNext()) {
            if (!consumer.test(nodeIterator.next())) {
                break;
            }
        }
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return new RandomLongIterator(nodeCount, new Random(System.currentTimeMillis()));
    }
}
