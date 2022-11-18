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
package org.neo4j.gds.steiner;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.procedures.LongProcedure;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.paths.delta.TentativeDistances;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.gds.steiner.SteinerBasedDeltaStepping.BIN_SIZE_THRESHOLD;
import static org.neo4j.gds.steiner.SteinerBasedDeltaStepping.NO_BIN;

class SteinerBasedDeltaTask implements Runnable {
    private final Graph graph;
    private final HugeLongArray frontier;
    private final TentativeDistances distances;
    private final double delta;
    private int binIndex;
    private final AtomicLong frontierIndex;
    private long frontierLength;
    private LongArrayList[] localBins;
    private SteinerBasedDeltaStepping.Phase phase = SteinerBasedDeltaStepping.Phase.RELAX;
    private final BitSet mergedToSource;

    SteinerBasedDeltaTask(
        Graph graph,
        HugeLongArray frontier,
        TentativeDistances distances,
        double delta,
        AtomicLong frontierIndex,
        BitSet mergedToSource
    ) {

        this.graph = graph.concurrentCopy();
        this.frontier = frontier;
        this.distances = distances;
        this.delta = delta;
        this.frontierIndex = frontierIndex;
        this.mergedToSource=mergedToSource;
        this.localBins = new LongArrayList[0];
    }

    @Override
    public void run() {
        if (phase == SteinerBasedDeltaStepping.Phase.RELAX) {
            relaxGlobalBin();
            relaxLocalBin();
        } else if (phase == SteinerBasedDeltaStepping.Phase.SYNC) {
            updateFrontier();
        }
    }

    void setPhase(SteinerBasedDeltaStepping.Phase phase) {
        this.phase = phase;
    }

    void setBinIndex(int binIndex) {
        this.binIndex = binIndex;
    }

    void setFrontierLength(long frontierLength) {
        this.frontierLength = frontierLength;
    }

    int minNonEmptyBin() {
        for (int i = binIndex; i < localBins.length; i++) {
            if (localBins[i] != null && !localBins[i].isEmpty()) {
                return i;
            }
        }
        return NO_BIN;
    }

    private void relaxGlobalBin() {
        long offset;
        while ((offset = frontierIndex.getAndAdd(64)) < frontierLength) {
            long limit = Math.min(offset + 64, frontierLength);

            for (long idx = offset; idx < limit; idx++) {
                var nodeId = frontier.get(idx);
                if (distances.distance(nodeId) >= delta * binIndex) {
                    relaxNode(nodeId);
                }
            }
        }
    }

    private void relaxLocalBin() {
        while (binIndex < localBins.length
               && localBins[binIndex] != null
               && !localBins[binIndex].isEmpty()
               && localBins[binIndex].size() < BIN_SIZE_THRESHOLD) {
            var binCopy = localBins[binIndex].clone();
            localBins[binIndex].elementsCount = 0;
            binCopy.forEach((LongProcedure) this::relaxNode);
        }
    }
    private void relaxNode(long nodeId) {
        graph.forEachRelationship(nodeId, 1.0, (sourceNodeId, targetNodeId, weight) -> {
            if (!mergedToSource.get(targetNodeId)) { //ignore merged vertices
                tryToUpdate(sourceNodeId, targetNodeId,weight);
            }
            return true;
        });
    }
    private void tryToUpdate(long sourceNodeId, long targetNodeId,double weight) {
        var oldDist = distances.distance(targetNodeId);
        var newDist = distances.distance(sourceNodeId) + weight;
        while (Double.compare(newDist, oldDist) < 0) {
            var witness = distances.compareAndExchange(targetNodeId, oldDist, newDist, sourceNodeId);

            if (Double.compare(witness, oldDist) == 0) {
                int destBin = (int) (newDist / delta);

                if (destBin >= localBins.length) {
                    this.localBins = Arrays.copyOf(localBins, destBin + 1);
                }
                if (localBins[destBin] == null) {
                    this.localBins[destBin] = new LongArrayList();
                }

                this.localBins[destBin].add(targetNodeId);
                break;
            }
            // CAX failed, retry
            oldDist = witness;
        }
    }

    private void updateFrontier() {
        if (binIndex < localBins.length && localBins[binIndex] != null && !localBins[binIndex].isEmpty()) {
            var size = localBins[binIndex].size();
            var offset = frontierIndex.getAndAdd(size);

            for (LongCursor longCursor : localBins[binIndex]) {
                long index = offset + longCursor.index;
                frontier.set(index, longCursor.value);
            }

            localBins[binIndex].elementsCount = 0;
        }
    }
}
