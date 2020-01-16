/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.labelpropagation;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

final class ComputeStepConsumer implements RelationshipWithPropertyConsumer {

    private final NodeProperties nodeWeights;
    private final HugeLongArray existingLabels;
    private final LongDoubleScatterMap votes;

    ComputeStepConsumer(
            NodeProperties nodeWeights,
            HugeLongArray existingLabels) {
        this.existingLabels = existingLabels;
        this.nodeWeights = nodeWeights;
        // use scatter map to get consistent (deterministic) hash order
        this.votes = new LongDoubleScatterMap();
    }

    @Override
    public boolean accept(final long sourceNodeId, final long targetNodeId, final double property) {
        castVote(targetNodeId, property);
        return true;
    }

    private void castVote(long candidate, double weight) {
        weight = weightOf(candidate, weight);
        long label = existingLabels.get(candidate);
        votes.addTo(label, weight);
    }

    private double weightOf(final long candidate, final double relationshipWeight) {
        double nodeWeight = nodeWeights.nodeProperty(candidate);
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
                if (vote.key < label) {
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
