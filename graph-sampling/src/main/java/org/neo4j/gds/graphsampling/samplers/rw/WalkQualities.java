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
package org.neo4j.gds.graphsampling.samplers.rw;

import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.neo4j.gds.graphsampling.samplers.rw.rwr.RandomWalkWithRestarts;

/**
 * In order be able to sample start nodes uniformly at random (for performance reasons) we have a special data
 * structure which is optimized for exactly this. In particular, we need to be able to do random access by index
 * of the set of start nodes we are currently interested in. A simple hashmap for example does not work for this
 * reason.
 */
public class WalkQualities {
    private final LongSet nodeIdIndex;
    private final LongArrayList nodeIds;
    private final DoubleArrayList qualities;
    private int size;
    private double sum;
    private double sumOfSquares;

    public WalkQualities(InitialStartQualities initialStartQualities) {
        this.nodeIdIndex = new LongHashSet(initialStartQualities.nodeIds());
        this.nodeIds = new LongArrayList(initialStartQualities.nodeIds());
        this.qualities = new DoubleArrayList(initialStartQualities.qualities());
        this.sum = qualities.size();
        this.sumOfSquares = qualities.size();
        this.size = qualities.size();
    }

    public boolean addNode(long nodeId) {
        if (nodeIdIndex.contains(nodeId)) {
            return false;
        }

        if (size >= nodeIds.size()) {
            nodeIds.add(nodeId);
            qualities.add(1.0);
        } else {
            nodeIds.set(size, nodeId);
            qualities.set(size, 1.0);
        }
        nodeIdIndex.add(nodeId);
        size++;

        sum += 1.0;
        sumOfSquares += 1.0;

        return true;
    }

    public void removeNode(int position) {
        double quality = qualities.get(position);
        sum -= quality;
        sumOfSquares -= quality * quality;

        nodeIds.set(position, nodeIds.get(size - 1));
        qualities.set(position, qualities.get(size - 1));
        size--;
    }

    public long nodeId(int position) {
        return nodeIds.get(position);
    }

    public double nodeQuality(int position) {
        return qualities.get(position);
    }

    public void updateNodeQuality(int position, double walkQuality) {
        double previousQuality = qualities.get(position);
        double updatedQuality = RandomWalkWithRestarts.QUALITY_MOMENTUM * previousQuality + (1 - RandomWalkWithRestarts.QUALITY_MOMENTUM) * walkQuality;

        sum += updatedQuality - previousQuality;
        sumOfSquares += updatedQuality * updatedQuality - previousQuality * previousQuality;

        qualities.set(position, updatedQuality);
    }

    public double expectedQuality() {
        if (size <= 0) {
            return 0;
        }
        return sumOfSquares / sum;
    }

    public int size() {
        return size;
    }
}
