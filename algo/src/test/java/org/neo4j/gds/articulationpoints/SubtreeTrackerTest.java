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
package org.neo4j.gds.articulationpoints;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


class SubtreeTrackerTest {

    @Test
    void shouldComputeStatisticsCorrectly(){
        var tracker =new SubtreeTracker(11);
        for (int i=0;i<11;++i){
            tracker.recordRoot(0,i);
        }

        tracker.recordSplitChild(1,3);
        tracker.recordSplitChild(4,5);
        tracker.recordSplitChild(4,6);
        tracker.recordSplitChild(4,10);
        tracker.recordSplitChild(1,4);
        tracker.recordSplitChild(0,1);
        //
        tracker.recordSplitChild(8,9);
        tracker.recordSplitChild(7,8);
        tracker.recordSplitChild(2,7);
        tracker.recordSplitChild(0,2);

        assertThat(tracker.maxComponentSize(0)).isEqualTo(6L);
        assertThat(tracker.minComponentSize(0)).isEqualTo(4L);
        assertThat(tracker.remainingComponents(0)).isEqualTo(2L);

    }

    @Test
    void shouldComputeStatisticsWithBackEdges(){
        var tracker =new SubtreeTracker(13);
        for (int i=0;i<13;++i){
            tracker.recordRoot(0,i);
        }

        tracker.recordSplitChild(3,7);
        tracker.recordSplitChild(3,8);

        tracker.recordSplitChild(4,10);
        tracker.recordSplitChild(4,11);
        tracker.recordJoinedChild(4,9);

        tracker.recordSplitChild(6,12);

        tracker.recordSplitChild(1,3);
        tracker.recordSplitChild(1,4);

        tracker.recordSplitChild(2,5);
        tracker.recordSplitChild(2,6);

        tracker.recordSplitChild(0,1);
        tracker.recordSplitChild(0,2);

        assertThat(tracker.maxComponentSize(4)).isEqualTo(10L);
        assertThat(tracker.minComponentSize(4)).isEqualTo(1L);
        assertThat(tracker.remainingComponents(4)).isEqualTo(3L);

    }

}
