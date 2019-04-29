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
package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.stream.Stream;

public abstract class LouvainAlgo<Self extends LouvainAlgo<Self>> extends Algorithm<Self> {

    static final PropertyTranslator<int[][]> COMMUNITIES_TRANSLATOR =
            (propertyId, allCommunities, nodeId) -> {
                // build int array
                int id = (int) nodeId;
                final int[] data = new int[allCommunities.length];
                for (int i = 0; i < data.length; i++) {
                    data[i] = allCommunities[i][id];
                }
                return Values.intArray(data);
            };

    static final PropertyTranslator<HugeLongArray[]> HUGE_COMMUNITIES_TRANSLATOR =
            (propertyId, allCommunities, nodeId) -> {
                // build int array
                final long[] data = new long[allCommunities.length];
                for (int i = 0; i < data.length; i++) {
                    data[i] = allCommunities[i].get(nodeId);
                }
                return Values.longArray(data);
            };


    /**
     * @return number of outer iterations
     */
    public abstract int getLevel();

    public abstract double[] getModularities();

    public double getFinalModularity() {
        double[] modularities = getModularities();
        return modularities[modularities.length - 1];
    }

    public abstract long communityCount();

    public abstract long communityIdOf(long node);

    public abstract void export(
            Exporter exporter,
            String propertyName,
            boolean includeIntermediateCommunities,
            String intermediateCommunitiesPropertyName);

    public abstract Stream<StreamingResult> dendrogramStream(boolean includeIntermediateCommunities);

    @SuppressWarnings("unchecked")
    @Override
    public final Self me() {
        return (Self) this;
    }

    /**
     * result object
     */
    public static final class Result {

        public final long nodeId;
        public final long community;

        public Result(long id, long community) {
            this.nodeId = id;
            this.community = community;
        }
    }

    public static final class StreamingResult {
        public final long nodeId;
        public final List<Long> communities;
        public final long community;

        public StreamingResult(long nodeId, List<Long> communities, long community) {
            this.nodeId = nodeId;
            this.communities = communities;
            this.community = community;
        }
    }
}
