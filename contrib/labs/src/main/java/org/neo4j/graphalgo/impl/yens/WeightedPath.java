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
package org.neo4j.graphalgo.impl.yens;

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.RawValues;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * array based path of nodes & a weight
 *
 * @author mknblch
 */
public class WeightedPath {

    public interface EdgeConsumer {
        void accept(int sourceNode, int targetNode);
    }

    private int[] nodes;
    private int offset = 0;
    private double weight = .0;

    public WeightedPath(int initialCapacity) {
        this(new int[initialCapacity], 0);
    }

    public WeightedPath(int[] data, int offset) {
        nodes = data;
        this.offset = offset;
    }

    // append a node to the path
    public void append(int nodeId) {
        nodes = ArrayUtil.grow(nodes, offset + 1);
        nodes[offset++] = nodeId;
    }

    /**
     * drop last element
     * @return this
     */
    public WeightedPath dropTail() {
        offset--;
        return this;
    }

    public int node(int index) {
        return nodes[index];
    }

    public int size() {
        return offset;
    }

    public WeightedPath withWeight(double weight) {
        this.weight = weight;
        return this;
    }

    public boolean containsNode(int node) {
        for (int i : nodes) {
            if (i == node) {
                return true;
            }
        }
        return false;
    }

    public void forEach(IntPredicate consumer) {
        for (int i = 0; i < offset; i++) {
            if (!consumer.test(nodes[i])) {
                return;
            }
        }
    }

    public void forEachEdge(EdgeConsumer consumer) {
        for (int i = 0; i < offset - 1; i++) {
            consumer.accept(nodes[i], nodes[i + 1]);
        }
    }

    public void forEachDo(IntConsumer consumer) {
        for (int i = 0; i < offset; i++) {
            consumer.accept(nodes[i]);
        }
    }

    public WeightedPath evaluateAndSetCost(RelationshipWeights weights) {
        this.weight = 0.;
        forEachEdge((a, b) -> {
            this.weight += weights.weightOf(a, b, 1.0D);
        });
        return this;
    }

    public double getCost() {
        return weight;
    }

    public WeightedPath pathTo(int end) {
        if (end > size()) {
            throw new ArrayIndexOutOfBoundsException();
        }
        return new WeightedPath(Arrays.copyOf(nodes, end + 1), end + 1);
    }

    public long edge(int i) {
        return RawValues.combineIntInt(nodes[i], nodes[i + 1]);
    }

    public WeightedPath reverse() {
        for (int i = 0; i < offset / 2; i++) {
            int temp = nodes[i];
            nodes[i] = nodes[offset - 1 - i];
            nodes[offset - 1 - i] = temp;
        }
        return this;
    }

    public String toString() {
        final StringBuilder buffer = new StringBuilder(size());
        forEachDo(n -> {
            if (buffer.length() != 0) {
                buffer.append(",");
            }
            buffer.append(n);
        });
        return buffer.toString();
    }

    public boolean elementWiseEquals(WeightedPath other, int length) {
        if (length == 0) {
            throw new IllegalArgumentException("length == 0");
        }
        if (size() < length || other.size() < length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (nodes[i] != other.nodes[i]) {
                return false;
            }
        }
        return true;
    }

    public int[] toArray() {
        return Arrays.copyOf(nodes, offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WeightedPath that = (WeightedPath) o;

        if (offset != that.offset) return false;
        return Arrays.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(nodes);
        result = 31 * result + offset;
        return result;
    }

    /**
     * concatenates another path to the and of this path
     * @return this
     */
    public WeightedPath concat(WeightedPath other) {
        nodes = ArrayUtil.grow(nodes, offset + other.size());
        System.arraycopy(other.nodes, 0, nodes, offset, other.size());
        offset += other.size();
        this.weight += other.weight;
        return this;
    }

    public static Comparator<WeightedPath> comparator() {
        return Comparator.comparingDouble(WeightedPath::getCost);
    }
}
