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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeObjectArray;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObjectNodePropertyValuesAdapterTest {

    @Test
    void shouldReturnNodePropertiesForFloatArrayValues() {
        HugeObjectArray<float[]> floats = HugeObjectArray.newArray(float[].class, 42);

        floats.setAll(idx -> {
            var values = new float[19];
            Arrays.fill(values, .37F);
            return values;
        });
        var nodeProperties = ObjectNodePropertyValuesAdapter.adapt(floats);
        assertThat(nodeProperties)
            .asInstanceOf(InstanceOfAssertFactories.type(FloatArrayNodePropertyValues.class))
            .describedAs("float properties must return the same size and elements as the underlying array")
            .returns(floats.size(), FloatArrayNodePropertyValues::nodeCount)
            .satisfies(array -> {
                for (int nodeId = 0; nodeId < 42; nodeId++) {
                    assertThat(array.floatArrayValue(nodeId))
                        .as("Elements for id %d must match", nodeId)
                        .containsExactly(floats.get(nodeId));
                }
            });
    }


    @Test
    void shouldReturnNodePropertiesForDoubleArrayValues() {
        var doubles = HugeObjectArray.newArray(double[].class, 42);
        doubles.setAll(idx -> {
            var values = new double[19];
            Arrays.fill(values, .99D);
            return values;
        });
        var nodeProperties = ObjectNodePropertyValuesAdapter.adapt(doubles);
        assertThat(nodeProperties)
            .asInstanceOf(InstanceOfAssertFactories.type(DoubleArrayNodePropertyValues.class))
            .describedAs("double properties must return the same size and elements as the underlying array")
            .returns(doubles.size(), DoubleArrayNodePropertyValues::nodeCount)
            .satisfies(array -> {
                for (int nodeId = 0; nodeId < 42; nodeId++) {
                    assertThat(array.doubleArrayValue(nodeId))
                        .as("Elements for id %d must match", nodeId)
                        .containsExactly(doubles.get(nodeId));
                }
            });
    }


    @Test
    void shouldReturnNodePropertiesForLongArrayValues() {
        var longs = HugeObjectArray.newArray(long[].class, 42);
        longs.setAll(idx -> {
            var values = new long[13];
            Arrays.fill(values, 99L);
            return values;
        });
        var nodeProperties = ObjectNodePropertyValuesAdapter.adapt(longs);
        assertThat(nodeProperties)
            .asInstanceOf(InstanceOfAssertFactories.type(LongArrayNodePropertyValues.class))
            .describedAs("long properties must return the same size and elements as the underlying array")
            .returns(longs.size(), LongArrayNodePropertyValues::nodeCount)
            .satisfies(array -> {
                for (int nodeId = 0; nodeId < 42; nodeId++) {
                    assertThat(array.longArrayValue(nodeId))
                        .as("Elements for id %d must match", nodeId)
                        .containsExactly(longs.get(nodeId));
                }
            });
    }

    @Test
    void shouldNotSupportNodePropertiesForStringValues() {
        assertThatThrownBy(() -> ObjectNodePropertyValuesAdapter.adapt(HugeObjectArray.newArray(String.class, 42)))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("This HugeObjectArray can not be converted to node properties.");
    }
}
