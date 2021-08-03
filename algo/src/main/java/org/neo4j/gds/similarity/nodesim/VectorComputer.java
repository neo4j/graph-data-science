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
package org.neo4j.gds.similarity.nodesim;

import com.carrotsearch.hppc.ArraySizingStrategy;
import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

abstract class VectorComputer {

    final Graph graph;
    LongArrayList targetIds;
    private long lastTarget = -1;

    VectorComputer(Graph graph) {
        this.graph = graph;
    }

    abstract void forEachRelationship(long node);

    public abstract double[] getWeights();

    void reset(int degree) {
        lastTarget = -1;
        targetIds = new LongArrayList(degree, ARRAY_SIZING_STRATEGY);
    }

    boolean consumeRelationship(long source, long target) {
        boolean consume = false;
        if (source != target && lastTarget != target) {
            consume = true;
        }
        lastTarget = target;
        return consume;
    }

    // The buffer is sized on the first call to the sizing strategy to hold exactly node degree elements
    private static final ArraySizingStrategy ARRAY_SIZING_STRATEGY =
        (currentBufferLength, elementsCount, degree) -> elementsCount + degree;

    static VectorComputer of(
        Graph graph,
        boolean weighted
    ) {
        return weighted
            ? new WeightedVectorComputer(graph)
            : new UnweightedVectorComputer(graph);
    }

    static final class UnweightedVectorComputer extends VectorComputer implements RelationshipConsumer {

        UnweightedVectorComputer(Graph graph) {
            super(graph);
        }

        @Override
        public boolean accept(long source, long target) {
            if (consumeRelationship(source, target)) {
                targetIds.add(target);
            }
            return true;
        }

        @Override
        public double[] getWeights() {
            throw new UnsupportedOperationException(formatWithLocale(
                "Method `getWeights` is not supported for %s",
                this.getClass().getSimpleName()
            ));
        }

        @Override
        void forEachRelationship(long node) {
            graph.forEachRelationship(node, this);
        }
    }

    static final class WeightedVectorComputer extends VectorComputer implements RelationshipWithPropertyConsumer {

        DoubleArrayList weights;

        WeightedVectorComputer(Graph graph) {
            super(graph);
        }

        @Override
        public boolean accept(long source, long target, double property) {
            if (consumeRelationship(source, target)) {
                targetIds.add(target);
                weights.add(property);
            }
            return true;
        }

        @Override
        public double[] getWeights() {
            return weights.buffer;
        }

        @Override
        void forEachRelationship(long node) {
            graph.forEachRelationship(node, 1.0D, this);
        }

        @Override
        void reset(int degree) {
            super.reset(degree);
            weights = new DoubleArrayList(degree, ARRAY_SIZING_STRATEGY);
        }
    }
}
