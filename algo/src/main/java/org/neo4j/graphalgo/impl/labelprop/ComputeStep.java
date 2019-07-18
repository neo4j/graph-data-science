package org.neo4j.graphalgo.impl.labelprop;

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
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
            HugeWeightMapping nodeWeights,
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
        localRelationshipIterator.forEachRelationship(nodeId, direction, consumer);
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
