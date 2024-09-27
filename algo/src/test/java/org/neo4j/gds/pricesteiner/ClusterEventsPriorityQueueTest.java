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

class ClusterEventsPriorityQueueTest {

    @Test
    void shouldWorkProperly(){

        var queue = new ClusterEventsPriorityQueue(4);

        queue.add(0,4);
        var val = queue.closestEvent( v -> true);
        assertThat(queue.topCluster()).isEqualTo(0);
        assertThat(val).isEqualTo(4.0);

        queue.add(1,3);
         val = queue.closestEvent( v -> true);
        assertThat(queue.topCluster()).isEqualTo(1);
        assertThat(val).isEqualTo(3.0);

        queue.add(2,0);
        val = queue.closestEvent( v -> true);
        assertThat(queue.topCluster()).isEqualTo(2);
        assertThat(val).isEqualTo(0.0);

        queue.pop();
        val = queue.closestEvent( v -> true);
        assertThat(queue.topCluster()).isEqualTo(1);
        assertThat(val).isEqualTo(3.0);

        val = queue.closestEvent( v -> v!=1);
        assertThat(queue.topCluster()).isEqualTo(0);
        assertThat(val).isEqualTo(4.0);
        
    }

}
