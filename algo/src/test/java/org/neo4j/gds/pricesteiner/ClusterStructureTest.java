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
        assertThat(clusterStructure.merge(0, 1)).isEqualTo(4);
        assertThat(clusterStructure.merge(2, 3)).isEqualTo(5);
        assertThat(clusterStructure.merge(4, 5)).isEqualTo(6);

        for (int i=0;i<6;++i) {
            assertThat(clusterStructure.sumOnEdgePart(i).cluster()).isEqualTo(6);
        }
    }

    @Test
    void shouldComputeMoatSumCorrectly(){
        var clusterStructure =new  ClusterStructure(4);

        clusterStructure.increaseMoat(0,3);
        clusterStructure.increaseMoat(1,2);
        clusterStructure.merge(0,1);
        clusterStructure.increaseMoat(4,3);

        assertThat(clusterStructure.sumOnEdgePart(0).totalMoat()).isEqualTo(6);

        clusterStructure.merge(2,3);
        clusterStructure.increaseMoat(4,10);
        clusterStructure.merge(4,5);

        clusterStructure.increaseMoat(6,100);

        assertThat(clusterStructure.sumOnEdgePart(0).totalMoat()).isEqualTo(116);

    }

    @Test
    void shouldReturnCorrectlyClusterPrizes(){
        var clusterStructure = new ClusterStructure(4);
        clusterStructure.setClusterPrize(0,10);
        clusterStructure.setClusterPrize(1,20);
        assertThat(clusterStructure.clusterPrize(0)).isEqualTo(10);
        assertThat(clusterStructure.clusterPrize(1)).isEqualTo(20);
        clusterStructure.merge(0,1);
        assertThat(clusterStructure.clusterPrize(4)).isEqualTo(30);
    }

    @Test
    void shouldComputeTightnessCorrectly(){
        var clusterStructure = new ClusterStructure(4);
        clusterStructure.setClusterPrize(0,10);
        clusterStructure.increaseMoat(0,3);
        clusterStructure.setClusterPrize(1,20);
        clusterStructure.increaseMoat(1,3);
        clusterStructure.merge(0,1);
        assertThat(clusterStructure.tightnessTime(4,3)).isEqualTo(27);
    }

}
