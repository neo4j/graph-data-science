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
package org.neo4j.gds.leiden;

import org.neo4j.gds.core.utils.paged.HugeLongArray;

public class LeidenDendrogramManager {

    private HugeLongArray[] dendrograms;
    private final long rootNodeCount;
    private final int maxIterations;
    private final boolean includeIntermediateCommunities;

    private int currentIndex;

    public LeidenDendrogramManager(long rootNodeCount, int maxIterations, boolean includeIntermediateCommunities) {
        if (includeIntermediateCommunities) {
            this.dendrograms = new HugeLongArray[maxIterations];
        } else {
            this.dendrograms = new HugeLongArray[1];
        }
        this.rootNodeCount = rootNodeCount;
        this.includeIntermediateCommunities = includeIntermediateCommunities;
        this.maxIterations = maxIterations;
    }

    public HugeLongArray[] getAllDendrograms() {
        return dendrograms;
    }

    public void prepareNextLevel(int iteration) {
        currentIndex = includeIntermediateCommunities ? iteration : 0;
        if (currentIndex > 0 || iteration == 0) {
            dendrograms[currentIndex] = HugeLongArray.newArray(rootNodeCount);
        }
    }

    public void set(long nodeId, long communityId) {
        dendrograms[currentIndex].set(nodeId, communityId);
    }

    public HugeLongArray getCurrent() {return dendrograms[currentIndex];}
}
