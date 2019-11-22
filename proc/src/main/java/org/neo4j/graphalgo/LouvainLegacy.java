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

package org.neo4j.graphalgo;

import java.util.ArrayList;
import java.util.List;

public class LouvainLegacy {
    public static class LegacyWriteResult {

        public static LegacyWriteResult fromWriteResult(LouvainProc.WriteResult result) {
            return new LegacyWriteResult(
                result.loadMillis,
                result.computeMillis,
                result.postProcessingMillis,
                result.writeMillis,
                result.nodes,
                result.communityCount,
                result.p100,
                result.p99,
                result.p95,
                result.p90,
                result.p75,
                result.p50,
                result.p25,
                result.p10,
                result.p5,
                result.p1,
                result.levels,
                result.modularities,
                result.modularity,
                result.write,
                result.writeProperty,
                result.includeIntermediateCommunities,
                result.writeProperty
            );
        }

        public static final LegacyWriteResult EMPTY = new LegacyWriteResult(
            0,
            0,
            0,
            0,
            0,
            0,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            0,
            new ArrayList<>(),
            -1,
            false,
            null,
            false,
            null);

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;
        public final long nodes;
        public final long communityCount;
        public final long iterations;
        public final List<Double> modularities;
        public final double modularity;
        public final long p1;
        public final long p5;
        public final long p10;
        public final long p25;
        public final long p50;
        public final long p75;
        public final long p90;
        public final long p95;
        public final long p99;
        public final long p100;
        public final boolean write;
        public final String writeProperty;
        public final boolean includeIntermediateCommunities;
        public final String intermediateCommunitiesWriteProperty;

        public LegacyWriteResult(
            long loadMillis,
            long computeMillis,
            long postProcessingMillis,
            long writeMillis,
            long nodes,
            long communityCount,
            long p100,
            long p99,
            long p95,
            long p90,
            long p75,
            long p50,
            long p25,
            long p10,
            long p5,
            long p1,
            long iterations,
            List<Double> modularities,
            double finalModularity,
            boolean write,
            String writeProperty,
            boolean includeIntermediateCommunities,
            String intermediateCommunitiesWriteProperty) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.writeMillis = writeMillis;
            this.nodes = nodes;
            this.communityCount = communityCount;
            this.p100 = p100;
            this.p99 = p99;
            this.p95 = p95;
            this.p90 = p90;
            this.p75 = p75;
            this.p50 = p50;
            this.p25 = p25;
            this.p10 = p10;
            this.p5 = p5;
            this.p1 = p1;
            this.iterations = iterations;
            this.modularities = new ArrayList<>(modularities.size());
            this.write = write;
            this.includeIntermediateCommunities = includeIntermediateCommunities;
            for (double mod : modularities) this.modularities.add(mod);
            this.modularity = finalModularity;
            this.writeProperty = writeProperty;
            this.intermediateCommunitiesWriteProperty = intermediateCommunitiesWriteProperty;
        }
    }

}
