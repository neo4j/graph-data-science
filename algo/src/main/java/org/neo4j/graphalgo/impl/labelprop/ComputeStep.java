package org.neo4j.graphalgo.impl.labelprop;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;

final class ComputeStep implements Step, WeightedRelationshipConsumer {

    private final RelationshipIterator localRelationshipIterator;
    private final Direction direction;
    private final HugeWeightMapping nodeWeights;
    private final HugeLongArray existingLabels;
    private final LongDoubleScatterMap votes;
    private final PrimitiveLongIterable nodes;
    private final ProgressLogger progressLogger;
    private final double maxNode;

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
        this.nodeWeights = nodeWeights;
        this.direction = direction;
        this.nodes = nodes;
        this.votes = new LongDoubleScatterMap();
    }

    boolean didChange = true;
    long iteration = 0L;

    @Override
    public final void run() {
        if (this.didChange) {
            iteration++;
            this.didChange = computeAll();
            if (!this.didChange) {
                release();
            }
        }
    }

    boolean computeAll() {
        return iterateAll(nodes.iterator());
    }

    void forEach(final long nodeId) {
        localRelationshipIterator.forEachRelationship(nodeId, direction, this);
    }

    double weightOf(final long candidate, final double relationshipWeight) {
        double nodeWeight = nodeWeights.nodeWeight(candidate);
        return relationshipWeight * nodeWeight;
    }

    @Override
    public boolean accept(final long sourceNodeId, final long targetNodeId, final double weight) {
        castVote(targetNodeId, weight);
        return true;
    }

    @Override
    public final Step next() {
        return this;
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

    final void castVote(long candidate, double weight) {
        weight = weightOf(candidate, weight);
        long label = existingLabels.get(candidate);
        votes.addTo(label, weight);
    }

    final boolean compute(long nodeId, boolean didChange) {
        votes.clear();
        long label = existingLabels.get(nodeId);
        long previous = label;
        forEach(nodeId);
        double weight = Double.NEGATIVE_INFINITY;
        for (LongDoubleCursor vote : votes) {
            if (weight < vote.value) {
                weight = vote.value;
                label = vote.key;
            } else if (weight == vote.value) {
                if (label > vote.key) {
                    label = vote.key;
                }
            }
        }
        if (label != previous) {
            existingLabels.set(nodeId, label);
            return true;
        }
        return didChange;
    }

    private static final long[] EMPTY_LONGS = new long[0];

    final void release() {
        // the HPPC release() method allocates new arrays
        // the clear() method overwrite the existing keys with the default value
        // we want to throw away all data to allow for GC collection instead.

        if (votes.keys != null) {
            votes.keys = EMPTY_LONGS;
            votes.clear();
            votes.keys = null;
            votes.values = null;
        }
    }
}
