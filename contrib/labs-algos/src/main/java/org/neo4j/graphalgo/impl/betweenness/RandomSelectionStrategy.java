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
package org.neo4j.graphalgo.impl.betweenness;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;

import java.security.SecureRandom;
import java.util.function.IntConsumer;

/**
 * Filters nodes randomly based on a given probability
 *
 * @author mknblch
 */
public class RandomSelectionStrategy implements RABrandesBetweennessCentrality.SelectionStrategy {

    private final SimpleBitSet bitSet;
    private final int size;

    public RandomSelectionStrategy(Graph graph) {
        this(graph, Math.log10(graph.nodeCount()) / Math.exp(2));
    }

    public RandomSelectionStrategy(Graph graph, double probability) {
        bitSet = new SimpleBitSet(Math.toIntExact(graph.nodeCount()));
        final SecureRandom random = new SecureRandom();
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (random.nextDouble() <= probability) {
                bitSet.put(i);
            }
        }
        size = bitSet.size();
    }

    @Override
    public boolean select(int nodeId) {
        return bitSet.contains(nodeId);
    }

    @Override
    public int size() {
        return size;
    }

}