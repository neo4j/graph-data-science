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
package org.neo4j.gds.pricesteiner;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.collections.ha.HugeDoubleArray;

import java.util.function.LongPredicate;

class ClusterActivity {

    private final BitSet activeClusters;
    private final HugeDoubleArray relevantTime;

    private long numberOfActiveClusters;
    private long maxActiveCluster;

    ClusterActivity(long nodeCount) {
        this.activeClusters = new BitSet(2 * nodeCount);
        this.relevantTime = HugeDoubleArray.newArray(2 * nodeCount);

        this.numberOfActiveClusters = nodeCount;
        this.maxActiveCluster = nodeCount;
        relevantTime.fill(0);
        activeClusters.set(0, nodeCount);
    }

    void deactivateCluster(long clusterId, double moat) {
        activeClusters.clear(clusterId);
        relevantTime.set(clusterId, moat);
        this.numberOfActiveClusters--;
    }

    void activateCluster(long clusterId, double moat) {
        activeClusters.set(clusterId);
        relevantTime.set(clusterId,moat);
        this.numberOfActiveClusters++;
        this.maxActiveCluster++;
    }

    long maxActiveCluster(){
        return  maxActiveCluster;
    }
    long numberOfActiveClusters() {
        return numberOfActiveClusters;
    }

    double relevantTime(long clusterId) {
        return relevantTime.get(clusterId);
    }

    LongPredicate active() {
        return activeClusters::get;
    }

    boolean active(long clusterId) {
        return activeClusters.get(clusterId);
    }

    long firstActiveCluster() {
        return activeClusters.nextSetBit(0);
    }

}
