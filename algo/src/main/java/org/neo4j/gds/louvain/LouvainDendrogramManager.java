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
package org.neo4j.gds.louvain;

import org.neo4j.gds.collections.ha.HugeLongArray;

public class LouvainDendrogramManager {

    private HugeLongArray[] dendrograms;
    private final long nodeCount;
    private final int maxLevels;
    private final boolean trackIntermediateCommunities;

    private int currentIndex;

    private int previousIndex;

    public LouvainDendrogramManager(long nodeCount, int maxLevels, boolean trackIntermediateCommunities) {
        if (trackIntermediateCommunities) {
            this.dendrograms = new HugeLongArray[maxLevels];
        } else {
            this.dendrograms = new HugeLongArray[Math.min(maxLevels, 2)];
        }
        this.nodeCount = nodeCount;
        this.trackIntermediateCommunities = trackIntermediateCommunities;
        this.maxLevels = maxLevels;
    }

    public HugeLongArray[] getAllDendrograms() {
        return dendrograms;
    }

    public void prepareNextLevel(int ranLevels) {
        currentIndex = trackIntermediateCommunities ? ranLevels : (ranLevels % 2);
        if (currentIndex > 1 || ranLevels <= 1) {
            dendrograms[currentIndex] = HugeLongArray.newArray(nodeCount);
        }
        previousIndex = trackIntermediateCommunities ? ranLevels - 1 : ((1 + ranLevels) % 2);
    }

    public void set(long nodeId, long communityId) {
        dendrograms[currentIndex].set(nodeId, communityId);
    }

    public HugeLongArray getCurrent() {
        return dendrograms[currentIndex];
    }

    public long getCommunity(long nodeId) {
        return dendrograms[currentIndex].get(nodeId);
    }

    public long getPrevious(long nodeId) {
        return dendrograms[previousIndex].get(nodeId);
    }

    public long[] getIntermediateCommunitiesForNode(long nodeId) {
        long[] communities = new long[dendrograms.length];

        for (int i = 0; i < dendrograms.length; i++) {
            communities[i] = dendrograms[i].get(nodeId);
        }

        return communities;
    }

    public void resizeDendrogram(int numLevels) {
        if (trackIntermediateCommunities) {
            HugeLongArray[] resizedDendrogram = new HugeLongArray[numLevels];
            if (numLevels < maxLevels) {
                System.arraycopy(this.dendrograms, 0, resizedDendrogram, 0, numLevels);
                this.dendrograms = resizedDendrogram;
            }
            this.currentIndex = numLevels - 1;
        } else {
            this.currentIndex = (1 + numLevels) % 2;
        }
    }
}
