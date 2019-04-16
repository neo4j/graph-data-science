/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import org.HdrHistogram.Histogram;
import org.junit.Test;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;

import static org.junit.Assert.assertEquals;

public class CommunityHistogramTest {

    @Test
    public void oneCommunityFromHugeMap() {
        HugeLongLongMap communitySizeMap = new HugeLongLongMap(AllocationTracker.EMPTY);
        communitySizeMap.addTo(1, 4);

        Histogram histogram = CommunityHistogram.buildFrom(communitySizeMap);

        assertEquals(4.0, histogram.getValueAtPercentile(100D), 0.01);
    }

    @Test
    public void multipleCommunitiesFromHugeMap() {
        HugeLongLongMap communitySizeMap = new HugeLongLongMap(AllocationTracker.EMPTY);
        communitySizeMap.addTo(1, 4);
        communitySizeMap.addTo(2, 10);
        communitySizeMap.addTo(3, 9);
        communitySizeMap.addTo(4, 8);
        communitySizeMap.addTo(5, 7);

        Histogram histogram = CommunityHistogram.buildFrom(communitySizeMap);

        assertEquals(10.0, histogram.getValueAtPercentile(100D), 0.01);
        assertEquals(8.0, histogram.getValueAtPercentile(50D), 0.01);
    }
}
