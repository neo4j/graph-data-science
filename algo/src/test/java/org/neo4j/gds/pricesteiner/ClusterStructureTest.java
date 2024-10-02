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

class ClusterStructureTest {

    @Test
    void shouldCreateNewClustersCorrectly() {
        var clusterStructure = new ClusterStructure(4);
        assertThat(clusterStructure.merge(0, 1,0)).isEqualTo(4);
        assertThat(clusterStructure.merge(2, 3,0)).isEqualTo(5);
        assertThat(clusterStructure.merge(4, 5,0)).isEqualTo(6);

        for (int i=0;i<6;++i) {
            assertThat(clusterStructure.sumOnEdgePart(i,10).cluster()).isEqualTo(6);
        }
    }

    @Test
    void shouldComputeMoatSumCorrectly(){
        var clusterStructure = new ClusterStructure(4);

        clusterStructure.merge(0,1,3); //cluster 4

        assertThat(clusterStructure.sumOnEdgePart(0,3).totalMoat()).isEqualTo(3);
        assertThat(clusterStructure.sumOnEdgePart(0,9).totalMoat()).isEqualTo(9);


        clusterStructure.merge(2,3,0); //cluster 5

        clusterStructure.merge(4,5,9);

        assertThat(clusterStructure.sumOnEdgePart(0,100).totalMoat()).isEqualTo(100);
        assertThat(clusterStructure.sumOnEdgePart(3,100).totalMoat()).isEqualTo(100);


    }

    @Test
    void shouldReturnCorrectlyClusterPrizes(){
        var clusterStructure = new ClusterStructure(4);
        clusterStructure.setClusterPrize(0,10);
        clusterStructure.setClusterPrize(1,20);
        assertThat(clusterStructure.clusterPrize(0)).isEqualTo(10);
        assertThat(clusterStructure.clusterPrize(1)).isEqualTo(20);
        clusterStructure.merge(0,1,0);
        assertThat(clusterStructure.clusterPrize(4)).isEqualTo(30);
    }

    @Test
    void shouldComputeTightnessCorrectly(){
        var clusterStructure = new ClusterStructure(4);
        clusterStructure.setClusterPrize(0,10);
        clusterStructure.setClusterPrize(1,20);
        clusterStructure.merge(0,1,3);
        assertThat(clusterStructure.tightnessTime(4,3)).isEqualTo(27);
    }

    @Test
    void shouldFindOriginalNodesOfCluster(){
        var clusterStructure = new ClusterStructure(4);
        clusterStructure.merge(0,1,0);
        clusterStructure.merge(4,3,0);
        var activePredicate = clusterStructure.activeOriginalNodesOfCluster(5);
        assertThat(activePredicate.get(0)).isTrue();
        assertThat(activePredicate.get(1)).isTrue();
        assertThat(activePredicate.get(3)).isTrue();
        assertThat(activePredicate.get(2)).isFalse();

        activePredicate = clusterStructure.activeOriginalNodesOfCluster(0);
        assertThat(activePredicate.get(0)).isTrue();
        assertThat(activePredicate.get(1)).isFalse();
        assertThat(activePredicate.get(3)).isFalse();
        assertThat(activePredicate.get(2)).isFalse();

    }

}
