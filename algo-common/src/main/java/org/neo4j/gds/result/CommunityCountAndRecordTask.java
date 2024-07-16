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
package org.neo4j.gds.result;

import org.HdrHistogram.Histogram;
import org.neo4j.gds.collections.hsa.HugeSparseLongArray;
import org.neo4j.gds.core.utils.partition.Partition;

class CommunityCountAndRecordTask implements Runnable {

    private final HugeSparseLongArray communitySizes;

    private final Partition partition;

    private final Histogram histogram;

    private long count;

    CommunityCountAndRecordTask(
        HugeSparseLongArray communitySizes,
        Partition partition,
        HistogramProvider histogramProvider
    ) {
        this.communitySizes = communitySizes;
        this.partition = partition;
        this.histogram = histogramProvider.get();
    }

    @Override
    public void run() {
        partition.consume(id -> {
            long communitySize = communitySizes.get(id);
            if (communitySize != CommunityStatistics.EMPTY_COMMUNITY) {
                count++;
                histogram.recordValue(communitySize);
            }
        });
    }

    public long count(){
        return count;
    }

    public Histogram histogram(){
        return histogram;
    }
}
