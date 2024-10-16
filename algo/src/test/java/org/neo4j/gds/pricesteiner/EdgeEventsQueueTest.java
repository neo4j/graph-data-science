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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EdgeEventsQueueTest {

    @Test
    void shouldActAsAQueue() {

        var edgeEventsQueue = new EdgeEventsQueue(10);

        edgeEventsQueue.addBothWays(0, 1, 10, 11, 10);
        edgeEventsQueue.addBothWays(1, 2, 20, 21, 20);
        edgeEventsQueue.addBothWays(4, 5, 30, 31, 5);
        edgeEventsQueue.addBothWays(6, 7, 40, 41, 40);

        edgeEventsQueue.performInitialAssignment(10);
        //CHECK 1
        assertThat(edgeEventsQueue.nextEventTime()).isEqualTo(5);
        var top1 = edgeEventsQueue.top();
        assertThat(top1).isIn(4L, 5L);
        var topEdge1 = edgeEventsQueue.topEdgePart();
        assertThat(topEdge1).isIn(30L, 31L);

        edgeEventsQueue.pop();
        assertThat(edgeEventsQueue.nextEventTime()).isEqualTo(5);
        var top2 = edgeEventsQueue.top();
        assertThat(top2).isIn(4L, 5L).isNotEqualTo(top1);
        var topEdge2 = edgeEventsQueue.topEdgePart();
        assertThat(topEdge2).isIn(30L, 31L).isNotEqualTo(topEdge1);
        //CHECK 2

        edgeEventsQueue.pop();
        assertThat(edgeEventsQueue.nextEventTime()).isEqualTo(10);
        var top3 = edgeEventsQueue.top();
        assertThat(top3).isIn(0L,1L);
        var topEdge3 = edgeEventsQueue.topEdgePart();
        assertThat(topEdge3).isIn(10L, 11L);

        edgeEventsQueue.pop();
        assertThat(edgeEventsQueue.nextEventTime()).isEqualTo(10);
        var top4 = edgeEventsQueue.top();
        assertThat(top4).isIn(0L,1L).isNotEqualTo(top1);
        var topEdge4 = edgeEventsQueue.topEdgePart();
        assertThat(topEdge4).isIn(10L, 11L).isNotEqualTo(topEdge3);
        //CHECK 3
        edgeEventsQueue.pop();
        assertThat(edgeEventsQueue.nextEventTime()).isEqualTo(20);
        var top5 = edgeEventsQueue.top();
        assertThat(top5).isIn(1L,2L);
        var topEdge5 = edgeEventsQueue.topEdgePart();
        assertThat(topEdge5).isIn(20L, 21L);

        edgeEventsQueue.pop();
        assertThat(edgeEventsQueue.nextEventTime()).isEqualTo(20);
        var top6 = edgeEventsQueue.top();
        assertThat(top6).isIn(1L,2L).isNotEqualTo(top5);
        var topEdge6 = edgeEventsQueue.topEdgePart();
        assertThat(topEdge6).isIn(20L, 21L).isNotEqualTo(topEdge5);

        //CHECK 4
        edgeEventsQueue.pop();
        assertThat(edgeEventsQueue.nextEventTime()).isEqualTo(40);
        var top7 = edgeEventsQueue.top();
        assertThat(top7).isIn(6L,7L);
        var topEdge7 = edgeEventsQueue.topEdgePart();
        assertThat(topEdge7).isIn(40L, 41L);

        edgeEventsQueue.pop();
        assertThat(edgeEventsQueue.nextEventTime()).isEqualTo(40);
        var top8 = edgeEventsQueue.top();
        assertThat(top8).isIn(6L,7L).isNotEqualTo(top7);
        var topEdge8 = edgeEventsQueue.topEdgePart();
        assertThat(topEdge8).isIn(40L, 41L).isNotEqualTo(topEdge7);


    }

    @Test
    void mergeShouldNotKeepOldCopiesAround(){
        var edgeEventsQueue = new EdgeEventsQueue(10);

        edgeEventsQueue.addBothWays(0, 1, 10, 11, 10);
        edgeEventsQueue.addBothWays(0, 1, 20, 21, 20);

        edgeEventsQueue.performInitialAssignment(10);

        edgeEventsQueue.mergeAndUpdate(11,0,1);

        edgeEventsQueue.pop();
        assertThat(edgeEventsQueue.currentlyActive()).isEqualTo(9L);
        assertThat(edgeEventsQueue.minOf(0)).isEqualTo(edgeEventsQueue.minOf(1)).isEqualTo(Double.MAX_VALUE);

        assertThat(edgeEventsQueue.top()).isEqualTo(11L);
        assertThat(edgeEventsQueue.nextEventTime()).isEqualTo(10);
        edgeEventsQueue.pop();
        assertThat(edgeEventsQueue.top()).isEqualTo(11L);
        assertThat(edgeEventsQueue.nextEventTime()).isEqualTo(20);
        edgeEventsQueue.pop();
        assertThat(edgeEventsQueue.top()).isEqualTo(11L);
        assertThat(edgeEventsQueue.nextEventTime()).isEqualTo(20);

    }

    @Test
    void shouldReAddCorrectly(){

        var edgeEventsQueue = new EdgeEventsQueue(2);
        edgeEventsQueue.addBothWays(0,1,0,1,10);
        edgeEventsQueue.performInitialAssignment(2);
        edgeEventsQueue.deactivateCluster(1);
        assertThat(edgeEventsQueue.top()).isEqualTo(0);
        edgeEventsQueue.pop();
        assertThat(edgeEventsQueue.nextEventTime()).isEqualTo(Double.MAX_VALUE);
        edgeEventsQueue.addWithCheck(0,0,10);
        assertThat(edgeEventsQueue.top()).isEqualTo(0);
        assertThat(edgeEventsQueue.nextEventTime()).isEqualTo(10);


    }

}
