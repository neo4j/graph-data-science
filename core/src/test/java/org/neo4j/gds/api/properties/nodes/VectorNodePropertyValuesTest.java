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
package org.neo4j.gds.api.properties.nodes;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class VectorNodePropertyValuesTest {

    private static FloatVectorNodePropertyValues floatVectorValues(float[]... values) {

        int[] array = Arrays.stream(values).mapToInt(f -> f.length).distinct().toArray();
        assertThat(array).hasSize(1);
        int dimension = array[0];

        return new FloatVectorNodePropertyValues() {
            @Override
            public int vectorDimension() {
                return dimension;
            }

            @Override
            public float[] floatArrayValue(long nodeId) {
                return values[(int) nodeId];
            }

            @Override
            public long nodeCount() {
                return values.length;
            }

            @Override
            public boolean hasValue(long nodeId) {
                return values[(int) nodeId] != null;
            }
        };
    }

    private static DoubleVectorNodePropertyValues doubleVectorValues(double[]... values) {
        int[] array = Arrays.stream(values).mapToInt(f -> f.length).distinct().toArray();
        assertThat(array).hasSize(1);
        int dimension = array[0];

        return new DoubleVectorNodePropertyValues() {
            @Override
            public int vectorDimension() {
                return dimension;
            }

            @Override
            public double[] doubleArrayValue(long nodeId) {
                return values[(int) nodeId];
            }

            @Override
            public long nodeCount() {
                return values.length;
            }
        };
    }

    @Test
    void reportsTheVectorValueTypeAndDeclaredDimension() {
        var vector = floatVectorValues(new float[]{1.0F, 2.0F, 3.0F});

        assertThat(vector.valueType()).isEqualTo(ValueType.FLOAT_VECTOR);
        assertThat(vector).isInstanceOf(VectorNodePropertyValues.class);
        assertThat(vector.vectorDimension()).isEqualTo(3);
    }

    @Test
    void answersTheDimensionWithoutReadingAValue() {
        var vector = new FloatVectorNodePropertyValues() {
            @Override
            public int vectorDimension() {
                return 8;
            }

            @Override
            public float[] floatArrayValue(long nodeId) {
                throw new AssertionError("the declared dimension must not require reading a value");
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };

        assertThat(vector.dimension()).hasValue(8);
        assertThat(vector.dimension(0)).hasValue(8);
    }

}
