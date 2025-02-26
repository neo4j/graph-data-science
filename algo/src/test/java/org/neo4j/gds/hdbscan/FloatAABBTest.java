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

import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;
import org.neo4j.gds.collections.ha.HugeLongArray;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SoftAssertionsExtension.class)
class FloatAABBTest {


    @ParameterizedTest
    @CsvSource({"10, 0", "100, 1"})
    void shouldComputeIndexCorrectly(int v,int expectedDimension){
        var aabb= new FloatAABB(new float[]{1,2,3},new float[]{10,v,10},3);
        assertThat(aabb.mostSpreadDimension()).isEqualTo(expectedDimension);
    }

    @Test
    void shouldConstructAABB(){
        var dim0 = new float[]{ 0.1f,  0.2f, 0.25f, 5.1f,  2.1f, 3.0f};
        var dim1 = new float[]{ 1.0f,  2.0f, 3.0f,  4.0f,  5.0f, 6.0f};
        var dim2 = new float[]{ 14.0f, 22.0f, 3.0f, -4.0f,  5.0f, 6.0f};

        var  nodeProperties =new FloatArrayNodePropertyValues(){

            @Override
            public float[] floatArrayValue(long nodeId) {
                return new float[]{dim0[(int)nodeId],dim1[(int)nodeId],dim2[(int)nodeId]};
            }

            @Override
            public long nodeCount() {
                return dim0.length;
            }
        };
        var ids = HugeLongArray.of(0,1,2,3,4,5);
        var aabb = FloatAABB.create(
            nodeProperties,
            ids,
            0,
            6,
            3
        );
        var min = aabb.min();
        assertThat(min).containsExactly(0.1f,1.0f,-4.0f);
        var max = aabb.max();
        assertThat(max).containsExactly(5.1f,6.0f,22.0f);

    }

}

