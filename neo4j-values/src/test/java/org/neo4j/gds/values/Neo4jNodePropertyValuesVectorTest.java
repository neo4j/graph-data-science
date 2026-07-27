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
import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.values.storable.Float32Vector;
import org.neo4j.values.storable.Float64Vector;
import org.neo4j.values.storable.Int64Vector;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Neo4jNodePropertyValuesVectorTest {

    @Test
    void floatArrayIsWrittenAsFloat32VectorWhenAsVector() {
        FloatArrayNodePropertyValues values = new FloatArrayNodePropertyValues() {
            @Override
            public float[] floatArrayValue(long nodeId) {
                return new float[]{1F, 2F, 3F};
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };

        var neo4jValues = Neo4jNodePropertyValuesUtil.of(values, true);

        assertThat(neo4jValues).isInstanceOf(Neo4jFloatArrayVectorNodePropertyValues.class);
        assertThat(neo4jValues.neo4jValue(0))
            .isInstanceOf(Float32Vector.class)
            .isEqualTo(Values.float32Vector(1F, 2F, 3F));
    }

    @Test
    void doubleArrayIsWrittenAsFloat64VectorWhenAsVector() {
        DoubleArrayNodePropertyValues values = new DoubleArrayNodePropertyValues() {
            @Override
            public double[] doubleArrayValue(long nodeId) {
                return new double[]{1D, 2D, 3D};
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };

        var neo4jValues = Neo4jNodePropertyValuesUtil.of(values, true);

        assertThat(neo4jValues).isInstanceOf(Neo4jDoubleArrayVectorNodePropertyValues.class);
        assertThat(neo4jValues.neo4jValue(0))
            .isInstanceOf(Float64Vector.class)
            .isEqualTo(Values.float64Vector(1D, 2D, 3D));
    }

    @Test
    void longArrayIsWrittenAsInt64VectorWhenAsVector() {
        LongArrayNodePropertyValues values = new LongArrayNodePropertyValues() {
            @Override
            public long[] longArrayValue(long nodeId) {
                return new long[]{1L, 2L, 3L};
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };

        var neo4jValues = Neo4jNodePropertyValuesUtil.of(values, true);

        assertThat(neo4jValues).isInstanceOf(Neo4jLongArrayVectorNodePropertyValues.class);
        assertThat(neo4jValues.neo4jValue(0))
            .isInstanceOf(Int64Vector.class)
            .isEqualTo(Values.int64Vector(1L, 2L, 3L));
    }

    @Test
    void floatArrayIsWrittenAsPlainArrayByDefault() {
        FloatArrayNodePropertyValues values = new FloatArrayNodePropertyValues() {
            @Override
            public float[] floatArrayValue(long nodeId) {
                return new float[]{1F, 2F, 3F};
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };

        var neo4jValues = Neo4jNodePropertyValuesUtil.of(values, false);

        assertThat(neo4jValues).isInstanceOf(Neo4jFloatArrayNodePropertyValues.class);
        assertThat(neo4jValues.neo4jValue(0)).isEqualTo(Values.floatArray(new float[]{1F, 2F, 3F}));
    }

    @Test
    void asVectorRejectsNonArrayTypes() {
        LongNodePropertyValues values = new LongNodePropertyValues() {
            @Override
            public long longValue(long nodeId) {
                return 42L;
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };

        assertThatThrownBy(() -> Neo4jNodePropertyValuesUtil.of(values, true))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("only supported for float, double or long array properties");
    }
}
