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

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeNodeProperties;
import org.neo4j.graphalgo.api.HugeRelationshipConsumer;
import org.neo4j.graphalgo.api.HugeRelationshipIterator;
import org.neo4j.graphalgo.api.HugeRelationshipWeights;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RandomLongIterable;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;

public final class HugeLabelPropagation extends BaseLabelPropagation<HugeGraph, HugeWeightMapping, HugeLabelPropagation> {

    private final ThreadLocal<HugeRelationshipIterator> localGraphs;

    public HugeLabelPropagation(
            HugeGraph graph,
            HugeNodeProperties nodeProperties,
            int batchSize,
            int concurrency,
            ExecutorService executor,
            AllocationTracker tracker) {
        super(
                graph,
                nodeProperties.hugeNodeProperties(PARTITION_TYPE),
                nodeProperties.hugeNodeProperties(WEIGHT_TYPE),
                batchSize,
                concurrency,
                executor,
                tracker);
        localGraphs = ThreadLocal.withInitial(graph::concurrentCopy);
    }

    @Override
    HugeLabelArray initialLabels(final long nodeCount, final AllocationTracker tracker) {
        return new HugeLabelArray(HugeLongArray.newArray(nodeCount, tracker));
    }

    @Override
    Initialization initStep(
            final HugeGraph graph,
            final Labels labels,
            final HugeWeightMapping nodeProperties,
            final HugeWeightMapping nodeWeights,
            final Direction direction,
            final ProgressLogger progressLogger,
            final RandomProvider randomProvider,
            final RandomLongIterable nodes) {
        return new InitStep(
                nodeProperties,
                labels,
                nodes,
                localGraphs,
                graph,
                nodeWeights,
                progressLogger,
                direction,
                graph.nodeCount() - 1L,
                randomProvider
        );
    }

    private static final class InitStep extends Initialization {

        private final HugeWeightMapping nodeProperties;
        private final Labels existingLabels;
        private final RandomLongIterable nodes;
        private final ThreadLocal<HugeRelationshipIterator> graph;
        private final HugeRelationshipWeights relationshipWeights;
        private final HugeWeightMapping nodeWeights;
        private final ProgressLogger progressLogger;
        private final Direction direction;
        private final long maxNode;
        private final RandomProvider random;

        private InitStep(
                HugeWeightMapping nodeProperties,
                Labels existingLabels,
                RandomLongIterable nodes,
                ThreadLocal<HugeRelationshipIterator> graph,
                HugeRelationshipWeights relationshipWeights,
                HugeWeightMapping nodeWeights,
                ProgressLogger progressLogger,
                Direction direction,
                long maxNode,
                RandomProvider random) {
            this.nodeProperties = nodeProperties;
            this.existingLabels = existingLabels;
            this.nodes = nodes;
            this.graph = graph;
            this.relationshipWeights = relationshipWeights;
            this.nodeWeights = nodeWeights;
            this.progressLogger = progressLogger;
            this.direction = direction;
            this.maxNode = maxNode;
            this.random = random;
        }

        @Override
        void setExistingLabels() {
            PrimitiveLongIterator iterator = nodes.iterator(random.randomForNewIteration());
            while (iterator.hasNext()) {
                long nodeId = iterator.next();
                long existingLabel = (long) nodeProperties.nodeWeight(nodeId, (double) nodeId);
                existingLabels.setLabelFor(nodeId, existingLabel);
            }
        }

        @Override
        Computation computeStep() {
            return new ComputeStep(
                    graph,
                    relationshipWeights,
                    nodeWeights,
                    progressLogger,
                    direction,
                    maxNode,
                    existingLabels,
                    nodes,
                    random
            );
        }
    }

    private static final class ComputeStep extends Computation implements HugeRelationshipConsumer {

        private final ThreadLocal<HugeRelationshipIterator> graphs;
        private final HugeRelationshipWeights relationshipWeights;
        private final HugeWeightMapping nodeWeights;
        private final Direction direction;
        private final RandomLongIterable nodes;
        private HugeRelationshipIterator graph;

        private ComputeStep(
                ThreadLocal<HugeRelationshipIterator> graphs,
                HugeRelationshipWeights relationshipWeights,
                HugeWeightMapping nodeWeights,
                ProgressLogger progressLogger,
                Direction direction,
                final long maxNode,
                Labels existingLabels,
                RandomLongIterable nodes,
                RandomProvider random) {
            super(existingLabels, progressLogger, maxNode, random);
            this.graphs = graphs;
            this.relationshipWeights = relationshipWeights;
            this.nodeWeights = nodeWeights;
            this.direction = direction;
            this.nodes = nodes;
        }

        @Override
        boolean computeAll() {
            graph = graphs.get();
            return iterateAll(nodes.iterator(randomProvider.randomForNewIteration()));
        }

        @Override
        void forEach(final long nodeId) {
            graph.forEachRelationship(nodeId, direction, this);
        }

        @Override
        double weightOf(final long nodeId, final long candidate) {
            double relationshipWeight = relationshipWeights.weightOf(nodeId, candidate);
            double nodeWeight = nodeWeights.nodeWeight(candidate);
            return relationshipWeight * nodeWeight;
        }

        @Override
        public boolean accept(final long sourceNodeId, final long targetNodeId) {
            castVote(sourceNodeId, targetNodeId);
            return true;
        }
    }
}
