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
package org.neo4j.gds.values;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.DoubleVectorNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.FloatVectorNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.VectorNodePropertyValues;
import org.neo4j.values.storable.Float32Vector;
import org.neo4j.values.storable.Float64Vector;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;

class Neo4jNodePropertyValuesVectorTest {

    private static FloatArrayNodePropertyValues floatArrayValues(float[] value) {
        return new FloatArrayNodePropertyValues() {
            @Override
            public float[] floatArrayValue(long nodeId) {
                return value;
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };
    }

    private static FloatVectorNodePropertyValues floatVectorValues(float[] value, int dimension) {
        return new FloatVectorNodePropertyValues() {
            @Override
            public int vectorDimension() {
                return dimension;
            }

            @Override
            public float[] floatArrayValue(long nodeId) {
                return value;
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };
    }

    private static DoubleArrayNodePropertyValues doubleArrayValues(double[] value) {
        return new DoubleArrayNodePropertyValues() {
            @Override
            public double[] doubleArrayValue(long nodeId) {
                return value;
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };
    }

    private static DoubleVectorNodePropertyValues doubleVectorValues(double[] value) {
        return new DoubleVectorNodePropertyValues() {
            @Override
            public int vectorDimension() {
                return value.length;
            }

            @Override
            public double[] doubleArrayValue(long nodeId) {
                return value;
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };
    }

    @Test
    void floatVectorIsMaterializedAsFloat32Vector() {
        var values = floatVectorValues(new float[]{1F, 2F, 3F}, 3);

        var neo4jValues = Neo4jNodePropertyValuesUtil.of(values);

        assertThat(neo4jValues).isInstanceOf(Neo4jFloatVectorNodePropertyValues.class);
        assertThat(neo4jValues.neo4jValue(0))
            .isInstanceOf(Float32Vector.class)
            .isEqualTo(Values.float32Vector(1F, 2F, 3F));
    }

    @Test
    void doubleVectorIsMaterializedAsFloat64Vector() {
        var values = doubleVectorValues(new double[]{1D, 2D, 3D});

        var neo4jValues = Neo4jNodePropertyValuesUtil.of(values);

        assertThat(neo4jValues).isInstanceOf(Neo4jDoubleVectorNodePropertyValues.class);
        assertThat(neo4jValues.neo4jValue(0))
            .isInstanceOf(Float64Vector.class)
            .isEqualTo(Values.float64Vector(1D, 2D, 3D));
    }

    @Test
    void plainArraysAreStillMaterializedAsArrays() {
        assertThat(Neo4jNodePropertyValuesUtil.of(floatArrayValues(new float[]{1F, 2F})).neo4jValue(0))
            .isEqualTo(Values.floatArray(new float[]{1F, 2F}));
        assertThat(Neo4jNodePropertyValuesUtil.of(doubleArrayValues(new double[]{1D, 2D})).neo4jValue(0))
            .isEqualTo(Values.doubleArray(new double[]{1D, 2D}));
    }

    @Test
    void aNullValueStaysNull() {
        var values = floatVectorValues(null, 1);

        assertThat(Neo4jNodePropertyValuesUtil.of(values).neo4jValue(0)).isNull();
    }

    @Test
    void materializationCarriesTheDeclaredDimension() {
        var values = floatVectorValues(new float[]{1F, 2F, 3F}, 3);

        var neo4jValues = Neo4jNodePropertyValuesUtil.of(values);

        assertThat(neo4jValues).isInstanceOf(VectorNodePropertyValues.class);
        assertThat(((VectorNodePropertyValues) neo4jValues).vectorDimension()).isEqualTo(3);
    }

    @Test
    void materializationIsIdempotent() {
        var values = floatVectorValues(new float[]{1F, 2F, 3F}, 3);

        var once = Neo4jNodePropertyValuesUtil.of(values);
        var twice = Neo4jNodePropertyValuesUtil.of(once);

        assertThat(twice.valueType()).isEqualTo(once.valueType());
        assertThat(twice.neo4jValue(0)).isEqualTo(once.neo4jValue(0));
    }
}
