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
package org.neo4j.graphalgo.impl.labelprop;

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;

final class ComputeStep implements Step {

    private final RelationshipIterator localRelationshipIterator;
    private final Direction direction;
    private final HugeLongArray existingLabels;
    private final PrimitiveLongIterable nodes;
    private final ProgressLogger progressLogger;
    private final double maxNode;
    private final ComputeStepConsumer consumer;

    ComputeStep(
            Graph graph,
            WeightMapping nodeWeights,
            ProgressLogger progressLogger,
            Direction direction,
            HugeLongArray existingLabels,
            PrimitiveLongIterable nodes) {
        this.existingLabels = existingLabels;
        this.progressLogger = progressLogger;
        this.maxNode = (double) graph.nodeCount() - 1L;
        this.localRelationshipIterator = graph.concurrentCopy();
        this.direction = direction;
        this.nodes = nodes;
        this.consumer = new ComputeStepConsumer(nodeWeights, existingLabels);
    }

    @Override
    public final Step next() {
        return this;
    }

    boolean didChange = true;
    long iteration = 0L;

    @Override
    public final void run() {
        if (this.didChange) {
            iteration++;
            this.didChange = iterateAll(nodes.iterator());
            if (!this.didChange) {
                release();
            }
        }
    }

    final boolean iterateAll(PrimitiveLongIterator nodeIds) {
        boolean didChange = false;
        while (nodeIds.hasNext()) {
            long nodeId = nodeIds.next();
            didChange = compute(nodeId, didChange);
            progressLogger.logProgress((double) nodeId, maxNode);
        }
        return didChange;
    }

    final boolean compute(long nodeId, boolean didChange) {
        consumer.clearVotes();
        long label = existingLabels.get(nodeId);
        localRelationshipIterator.forEachRelationship(nodeId, direction, LabelPropagation.DEFAULT_WEIGHT, consumer);
        long newLabel = consumer.tallyVotes(label);
        if (newLabel != label) {
            existingLabels.set(nodeId, newLabel);
            return true;
        }
        return didChange;
    }

    final void release() {
        consumer.release();
    }
}
