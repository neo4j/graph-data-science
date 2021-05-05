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
package org.neo4j.gds.ml.linkmodels;

import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.DoubleStream;

/**
 * Represents a sorted set of doubles, sorted according to their absolute value in increasing order.
 */
public final class SignedProbabilities {
    private static final Comparator<Double> ABSOLUTE_VALUE_COMPARATOR = Comparator.comparingDouble(Math::abs);

    private final Optional<TreeSet<Double>> tree;
    private final Optional<List<Double>> list;
    private final boolean isTree;
    private long positiveCount;
    private long negativeCount;

    public static long estimateMemory(GraphDimensions dimensions, RelationshipType relationshipType, double relationshipFraction) {
        var relationshipCount = dimensions.relationshipCounts().get(relationshipType) * relationshipFraction;
        return MemoryUsage.sizeOfInstance(SignedProbabilities.class) +
               MemoryUsage.sizeOfInstance(Optional.class) +
               MemoryUsage.sizeOfInstance(ArrayList.class) +
               MemoryUsage.sizeOfInstance(Double.class) * ((long) relationshipCount);
    }

    private SignedProbabilities(Optional<TreeSet<Double>> tree, Optional<List<Double>> list, boolean isTree) {
        this.tree = tree;
        this.list = list;
        this.isTree = isTree;
    }

    public static SignedProbabilities create(long capacity) {
        var isTree = capacity > Integer.MAX_VALUE;
        Optional<TreeSet<Double>> tree = isTree ? Optional.of(new TreeSet<>(ABSOLUTE_VALUE_COMPARATOR)) : Optional.empty();
        Optional<List<Double>> list = !isTree ? Optional.of(new ArrayList<>((int) capacity)) : Optional.empty();
        return new SignedProbabilities(tree, list, isTree);
    }

    public synchronized void add(double value) {
        if (value > 0) positiveCount++;
        else negativeCount++;
        if (isTree) {
            tree.get().add(value);
        } else {
            list.get().add(value);
        }
    }

    public DoubleStream stream() {
        if (isTree) {
            return tree.get().stream().mapToDouble(d -> d);
        } else {
            Collections.sort(list.get(), ABSOLUTE_VALUE_COMPARATOR);
            return list.get().stream().mapToDouble(d -> d);
        }
    }

    public long positiveCount() {
        return positiveCount;
    }

    public long negativeCount() {
        return negativeCount;
    }
}
