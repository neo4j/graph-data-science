package org.neo4j.graphalgo.impl.labelprop;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

final class ComputeStepConsumer implements WeightedRelationshipConsumer {

    private final HugeWeightMapping nodeWeights;
    private final HugeLongArray existingLabels;
    private final LongDoubleScatterMap votes;

    ComputeStepConsumer(
            HugeWeightMapping nodeWeights,
            HugeLongArray existingLabels) {
        this.existingLabels = existingLabels;
        this.nodeWeights = nodeWeights;
        // use scatter map to get consistent (deterministic) hash order
        this.votes = new LongDoubleScatterMap();
    }

    @Override
    public boolean accept(final long sourceNodeId, final long targetNodeId, final double weight) {
        castVote(targetNodeId, weight);
        return true;
    }

    void castVote(long candidate, double weight) {
        weight = weightOf(candidate, weight);
        long label = existingLabels.get(candidate);
        votes.addTo(label, weight);
    }

    double weightOf(final long candidate, final double relationshipWeight) {
        double nodeWeight = nodeWeights.nodeWeight(candidate);
        return relationshipWeight * nodeWeight;
    }

    void clearVotes() {
        votes.clear();
    }

    long tallyVotes(long label) {
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
        return label;
    }

    private static final long[] EMPTY_LONGS = new long[0];

    void release() {
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
