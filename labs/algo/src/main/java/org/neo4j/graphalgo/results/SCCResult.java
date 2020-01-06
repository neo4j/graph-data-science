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
package org.neo4j.graphalgo.results;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

public class SCCResult {

    public static SCCResult EMPTY = new SCCResult(
        0, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 0,
        false, null, null
    );

    public final long loadMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long postProcessingMillis;
    public final long nodes;
    public final long communityCount;
    public final long setCount;
    public final long minSetSize;
    public final long maxSetSize;
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
    public final String partitionProperty;
    public final String writeProperty;

    public SCCResult(
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
        long minSetSize,
        long maxSetSize,
        boolean write,
        String partitionProperty,
        String writeProperty
    ) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.postProcessingMillis = postProcessingMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
        this.setCount = this.communityCount = communityCount;
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
        this.minSetSize = minSetSize;
        this.maxSetSize = maxSetSize;
        this.write = write;
        this.partitionProperty = partitionProperty;
        this.writeProperty = writeProperty;
    }

    public static final class Builder extends AbstractCommunityResultBuilder<SCCResult> {
        private String partitionProperty;
        private String writeProperty;

        public Builder(
            boolean buildHistogram,
            boolean buildCommunityCount,
            AllocationTracker tracker
        ) {
            super(buildHistogram, buildCommunityCount, tracker);
        }

        @Override
        public SCCResult buildResult() {
            return new SCCResult(
                loadMillis,
                computeMillis,
                writeMillis,
                postProcessingDuration,
                nodeCount,
                maybeCommunityCount.orElse(0),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(100)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(99)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(95)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(90)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(75)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(50)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(25)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(10)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(5)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(1)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getMinNonZeroValue()).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getMaxValue()).orElse(0L),
                write,
                partitionProperty, writeProperty
            );
        }

        public Builder withPartitionProperty(String partitionProperty) {
            this.partitionProperty = partitionProperty;
            return this;
        }

        public Builder withWriteProperty(String writeProperty) {
            this.writeProperty = writeProperty;
            return this;
        }
    }

}
