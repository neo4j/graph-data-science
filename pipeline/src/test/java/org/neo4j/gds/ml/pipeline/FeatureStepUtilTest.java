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
package org.neo4j.gds.ml.pipeline;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.DoubleVectorNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.FloatVectorNodePropertyValues;

import static org.assertj.core.api.Assertions.assertThat;

class FeatureStepUtilTest {

    private static final float[] FLOATS = {1.0F, 2.0F, 3.0F};
    private static final double[] DOUBLES = {1.0D, 2.0D, 3.0D};

    @Test
    void aFloatArrayPropertyHasTheDimensionOfItsValues() {
        var values = new FloatArrayNodePropertyValues() {
            @Override
            public float[] floatArrayValue(long nodeId) {
                return FLOATS;
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };

        assertThat(FeatureStepUtil.propertyDimension(values, "embedding")).isEqualTo(3);
    }

    @Test
    void aFloatVectorPropertyHasTheSameDimensionAsTheEquivalentArray() {
        var values = new FloatVectorNodePropertyValues() {
            @Override
            public float[] floatArrayValue(long nodeId) {
                return FLOATS;
            }

            @Override
            public int vectorDimension() {
                return FLOATS.length;
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };

        assertThat(FeatureStepUtil.propertyDimension(values, "embedding")).isEqualTo(3);
    }

    @Test
    void aDoubleArrayPropertyHasTheDimensionOfItsValues() {
        var values = new DoubleArrayNodePropertyValues() {
            @Override
            public double[] doubleArrayValue(long nodeId) {
                return DOUBLES;
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };

        assertThat(FeatureStepUtil.propertyDimension(values, "embedding")).isEqualTo(3);
    }

    @Test
    void aDoubleVectorPropertyHasTheSameDimensionAsTheEquivalentArray() {
        var values = new DoubleVectorNodePropertyValues() {
            @Override
            public double[] doubleArrayValue(long nodeId) {
                return DOUBLES;
            }

            @Override
            public int vectorDimension() {
                return DOUBLES.length;
            }

            @Override
            public long nodeCount() {
                return 1;
            }
        };

        assertThat(FeatureStepUtil.propertyDimension(values, "embedding")).isEqualTo(3);
    }
}
