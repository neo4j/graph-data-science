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
package org.neo4j.gds.hdbscan;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClosestDistanceInformationTrackerTest {



    @Test
    void shouldTrackInformationCorrectly(){

        var  tracker = ClosestDistanceInformationTracker.create(10);
        tracker.consider(0,1,0,1,10.0);
        tracker.consider(0,3,2,5,5.0);
        tracker.consider(2,1,3,6,11.0);

        assertThat(tracker.componentClosestDistance(0)).isEqualTo(5.0);
        assertThat(tracker.componentClosestDistance(1)).isEqualTo(10.0);
        assertThat(tracker.componentClosestDistance(2)).isEqualTo(11.0);
        assertThat(tracker.componentClosestDistance(3)).isEqualTo(5.0);


        assertThat(tracker.componentInsideBestNode(0)).isEqualTo(2);
        assertThat(tracker.componentInsideBestNode(1)).isEqualTo(1);
        assertThat(tracker.componentInsideBestNode(2)).isEqualTo(3);
        assertThat(tracker.componentInsideBestNode(3)).isEqualTo(5);


        assertThat(tracker.componentOutsideBestNode(0)).isEqualTo(5);
        assertThat(tracker.componentOutsideBestNode(1)).isEqualTo(0);
        assertThat(tracker.componentOutsideBestNode(2)).isEqualTo(6);
        assertThat(tracker.componentOutsideBestNode(3)).isEqualTo(2);

    }

    @Test
    void shouldResetBounds(){

        var  tracker = ClosestDistanceInformationTracker.create(10);
        tracker.consider(0,1,0,1,10);
        tracker.reset(1);
        assertThat(tracker.componentClosestDistance(0)).isEqualTo(Double.MAX_VALUE);
        assertThat(tracker.componentInsideBestNode(0)).isEqualTo(-1L);
        assertThat(tracker.componentOutsideBestNode(0)).isEqualTo(-1L);

        assertThat(tracker.componentClosestDistance(1)).isEqualTo(10.0);
        assertThat(tracker.componentInsideBestNode(1)).isEqualTo(1);
        assertThat(tracker.componentOutsideBestNode(1)).isEqualTo(0);

    }

}
