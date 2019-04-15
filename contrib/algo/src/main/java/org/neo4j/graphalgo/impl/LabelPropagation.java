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
package org.neo4j.graphalgo.impl;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RandomIntIterable;
import org.neo4j.graphalgo.core.utils.RandomLongIterable;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;

public final class LabelPropagation extends BaseLabelPropagation<Graph, WeightMapping, LabelPropagation> {

    public LabelPropagation(
            Graph graph,
            NodeProperties nodeProperties,
            int batchSize,
            int concurrency,
            ExecutorService executor) {
        super(
                graph,
                nodeProperties.nodeProperties(PARTITION_TYPE),
                nodeProperties.nodeProperties(WEIGHT_TYPE),
                batchSize,
                concurrency,
                executor,
                AllocationTracker.EMPTY);
    }

    @Override
    LabelArray initialLabels(final long nodeCount, final AllocationTracker tracker) {
        return new LabelArray(new long[Math.toIntExact(nodeCount)]);
    }

    @Override
    Initialization initStep(
            final Graph graph,
            final Labels labels,
            final WeightMapping nodeProperties,
            final WeightMapping nodeWeights,
            final Direction direction,
            final ProgressLogger progressLogger,
            final RandomProvider randomProvider,
            final RandomLongIterable nodes) {
        return new InitStep(
                graph,
                labels,
                direction,
                randomProvider,
                getProgressLogger(),
                new RandomIntIterable(nodes),
                nodeProperties,
                nodeWeights
        );
    }

    private static final class InitStep extends Initialization {

        private final Graph graph;
        private final Labels existingLabels;
        private final Direction direction;
        private final RandomProvider random;
        private final ProgressLogger progressLogger;
        private final RandomIntIterable nodes;
        private final WeightMapping nodeProperties;
        private final WeightMapping nodeWeights;

        private InitStep(
                Graph graph,
                Labels existingLabels,
                Direction direction,
                RandomProvider random,
                ProgressLogger progressLogger,
                RandomIntIterable nodes,
                WeightMapping nodeProperties,
                WeightMapping nodeWeights) {
            this.graph = graph;
            this.existingLabels = existingLabels;
            this.direction = direction;
            this.random = random;
            this.progressLogger = progressLogger;
            this.nodes = nodes;
            this.nodeProperties = nodeProperties;
            this.nodeWeights = nodeWeights;
        }

        @Override
        void setExistingLabels() {
            PrimitiveIntIterator iterator = nodes.iterator();
            while (iterator.hasNext()) {
                int nodeId = iterator.next();
                long existingLabel = (long) this.nodeProperties.get(nodeId, (double) nodeId);
                existingLabels.setLabelFor(nodeId, existingLabel);
            }
        }

        @Override
        Computation computeStep() {
            return new ComputeStep(
                    graph,
                    existingLabels,
                    direction,
                    random,
                    progressLogger,
                    nodes,
                    nodeWeights);
        }
    }

    private static final class ComputeStep extends Computation implements RelationshipConsumer {

        private final Graph graph;
        private final Direction direction;
        private final RandomIntIterable nodes;
        private final WeightMapping nodeWeights;

        private ComputeStep(
                Graph graph,
                Labels existingLabels,
                Direction direction,
                RandomProvider random,
                ProgressLogger progressLogger,
                RandomIntIterable nodes,
                WeightMapping nodeWeights) {
            super(existingLabels, progressLogger, graph.nodeCount() - 1L, random);
            this.graph = graph;
            this.direction = direction;
            this.nodes = nodes;
            this.nodeWeights = nodeWeights;
        }

        @Override
        boolean computeAll() {
            return iterateAll(nodes.iterator(randomProvider.randomForNewIteration()));
        }

        @Override
        void forEach(final long nodeId) {
            graph.forEachRelationship((int) nodeId, direction, this);
        }

        @Override
        double weightOf(final long nodeId, final long candidate) {
            double relationshipWeight = graph.weightOf((int) nodeId, (int) candidate);
            double nodeWeight = nodeWeights.get((int) candidate);
            return relationshipWeight * nodeWeight;
        }

        @Override
        public boolean accept(
                final int sourceNodeId,
                final int targetNodeId,
                final long relationId) {
            castVote((long) sourceNodeId, (long) targetNodeId);
            return true;
        }
    }
}
