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

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SoftAssertionsExtension.class)
class AABBTest {


    @ParameterizedTest
    @CsvSource({"10, 0", "100, 1"})
    void shouldComputeIndexCorrectly(int v,int expectedDimension){
        var aabb= new AABB(new double[]{1,2,3},new double[]{10,v,10},3);
        assertThat(aabb.mostSpreadDimension()).isEqualTo(expectedDimension);
    }

    @Test
    void shouldConstructAABB(){
        var dim0 = new double[]{ 0.1,  0.2, 0.25, 5.1,  2.1, 3.0};
        var dim1 = new double[]{ 1.0,  2.0, 3.0,  4.0,  5.0, 6.0};
        var dim2 = new double[]{ 14.0, 22.0, 3.0, -4.0,  5.0, 6.0};

        var  nodeProperties =new DoubleArrayNodePropertyValues(){

            @Override
            public double[] doubleArrayValue(long nodeId) {
                return new double[]{dim0[(int)nodeId],dim1[(int)nodeId],dim2[(int)nodeId]};
            }

            @Override
            public long nodeCount() {
                return dim0.length;
            }
        };
        var ids = HugeLongArray.of(0,1,2,3,4,5);
        var aabb = AABB.create(
            nodeProperties,
            ids,
            0,
            6,
            3
        );
        var min =aabb.min();
        assertThat(min).containsExactly(0.1,1.0,-4.0);
        var max =aabb.max();
        assertThat(max).containsExactly(5.1,6.0,22.0);

    }

    @Test
    void shouldConstructAABBInParallel(SoftAssertions assertions){
        var dim0 = new double[]{ 0.1,  0.2, 0.25, 5.1,  2.1, 3.0};
        var dim1 = new double[]{ 1.0,  2.0, 3.0,  4.0,  5.0, 6.0};
        var dim2 = new double[]{ 14.0, 22.0, 3.0, -4.0,  5.0, 6.0};

        var  nodeProperties =new DoubleArrayNodePropertyValues(){

            @Override
            public double[] doubleArrayValue(long nodeId) {
                return new double[]{dim0[(int)nodeId],dim1[(int)nodeId],dim2[(int)nodeId]};
            }

            @Override
            public long nodeCount() {
                return dim0.length;
            }
        };
        var ids = HugeLongArray.of(0,1,2,3,4,5);
        var aabb = AABB.createInParallel(
            nodeProperties,
            ids,
            0,
            6,
            3,
            new Concurrency(2)
        );
        var min =aabb.min();
        assertions.assertThat(min).containsExactly(0.1,1.0,-4.0);
        var max =aabb.max();
        assertions.assertThat(max).containsExactly(5.1,6.0,22.0);

    }

}
