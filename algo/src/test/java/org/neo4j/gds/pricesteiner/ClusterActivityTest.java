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

class ClusterActivityTest {

    @Test
    void shouldWorkAsExpected(){
        var clusterActivity =new ClusterActivity(10);
        assertThat(clusterActivity.relevantTime(0)).isEqualTo(0);
        assertThat(clusterActivity.numberOfActiveClusters()).isEqualTo(10);

        clusterActivity.deactivateCluster(0,5);
        assertThat(clusterActivity.relevantTime(0)).isEqualTo(5);
        assertThat(clusterActivity.numberOfActiveClusters()).isEqualTo(9);

        clusterActivity.deactivateCluster(1,5);
        assertThat(clusterActivity.numberOfActiveClusters()).isEqualTo(8);

        clusterActivity.activateCluster(11,5);
        assertThat(clusterActivity.relevantTime(11)).isEqualTo(5);
        assertThat(clusterActivity.numberOfActiveClusters()).isEqualTo(9);

        assertThat(clusterActivity.active(0)).isFalse();
        assertThat(clusterActivity.active(1)).isFalse();
        assertThat(clusterActivity.active(2)).isTrue();
        assertThat(clusterActivity.active(11)).isTrue();

        clusterActivity.deactivateCluster(11,15);
        assertThat(clusterActivity.numberOfActiveClusters()).isEqualTo(8);

        assertThat(clusterActivity.relevantTime(11)).isEqualTo(15);
    }

    @Test
    void  shouldFindSingleActiveNodeCorrectly(){
        var clusterActivity =new ClusterActivity(4);
        clusterActivity.deactivateCluster(0,1);
        clusterActivity.deactivateCluster(1,1);
        clusterActivity.deactivateCluster(3,1);

        assertThat(clusterActivity.firstActiveCluster()).isEqualTo(2);
        clusterActivity.activateCluster(0,1); //this does not happen  in practice but oh well it is a test
        clusterActivity.deactivateCluster(2,1);
        assertThat(clusterActivity.firstActiveCluster()).isEqualTo(0);

    }

}
