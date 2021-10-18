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
package org.neo4j.gds.api;

import com.carrotsearch.hppc.BitSet;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.loading.BitIdMap;
import org.neo4j.gds.core.loading.LabelInformation;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeSparseLongArray;
import org.neo4j.gds.core.utils.paged.SparseLongArray;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UnionNodePropertiesTest {

    @Test
    void shouldReturnDoubleCorrectly() {
        var unionNodeProperties = initializeUnionNodeProperties(Values.of(42.0D), DefaultValue.of(0.0D));
        Assertions.assertThat(unionNodeProperties.doubleValue(0)).isEqualTo(42.0D);
    }

    @Test
    void shouldReturnLongCorrectly() {
        var unionNodeProperties = initializeUnionNodeProperties(Values.of(42L), DefaultValue.of(0.0D));
        Assertions.assertThat(unionNodeProperties.longValue(0)).isEqualTo(42L);
    }

    @Test
    void shouldReturnFloatArrayCorrectly() {
        var unionNodeProperties = initializeUnionNodeProperties(
            Values.of(new float[]{4.2F}),
            DefaultValue.of(new float[]{})
        );

        Assertions.assertThat(unionNodeProperties.floatArrayValue(0)).isEqualTo(new float[]{4.2f});
    }

    @Test
    void shouldReturnDoubleArrayCorrectly() {
        var unionNodeProperties = initializeUnionNodeProperties(Values.of(
            new double[]{4.2}),
            DefaultValue.of(new double[]{})
        );

        Assertions.assertThat(unionNodeProperties.doubleArrayValue(0)).isEqualTo(new double[]{4.2});
    }

    @Test
    void shouldReturnLongArrayCorrectly() {
        var unionNodeProperties = initializeUnionNodeProperties(
            Values.of(new long[]{42L}),
            DefaultValue.of(new long[]{})
        );

        Assertions.assertThat(unionNodeProperties.longArrayValue(0)).isEqualTo(new long[]{42L});
    }

    @Test
    void shouldCalculateMaximumLongValue() {
        var unionNodeProperties = initializeUnionNodeProperties(Values.of(1337L), DefaultValue.of(42L));

        assertThat(unionNodeProperties.getMaxLongPropertyValue())
            .isPresent()
            .hasValue(1337L);
    }

    @Test
    void shouldCalculateMaximumDoubleValue() {
        var unionNodeProperties = initializeUnionNodeProperties(Values.of(1337.0), DefaultValue.of(42.0));

        assertThat(unionNodeProperties.getMaxDoublePropertyValue())
            .isPresent()
            .hasValue(1337.0);
    }

    @Test
    void shouldThrowConversionException() {
        var unionNodeProperties = initializeUnionNodeProperties(
            Values.of(new double[]{1.337D}),
            DefaultValue.of(new double[]{})
        );

        assertThatThrownBy(() -> unionNodeProperties.doubleValue(0))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cannot cast properties of type DOUBLE_ARRAY")
            .hasMessageEndingWith("to double");
    }

    private UnionNodeProperties initializeUnionNodeProperties(Value propertyValue, DefaultValue defaultValue) {
        NodePropertiesFromStoreBuilder doubleNodePropertiesBuilder = NodePropertiesFromStoreBuilder.of(
            1, AllocationTracker.empty(), defaultValue
        );

        doubleNodePropertiesBuilder.set(0, propertyValue);

        NodeProperties doubleNodeProperties = doubleNodePropertiesBuilder.build();
        NodeLabel label = NodeLabel.of("label");
        Map<NodeLabel, NodeProperties> propertiesMap = new HashMap<>();
        propertiesMap.put(label, doubleNodeProperties);

        var sparseLongArrayBuilder = SparseLongArray.sequentialBuilder(1);
        sparseLongArrayBuilder.set(0);
        var sparseLongArray = sparseLongArrayBuilder.build();

        HugeLongArray graphIds = HugeLongArray.newArray(1, AllocationTracker.empty());
        graphIds.setAll(i -> i);

        HugeSparseLongArray.Builder builder = HugeSparseLongArray.builder(1, AllocationTracker.empty());
        builder.set(0, 0);

        HashMap<NodeLabel, BitSet> bitSets = new HashMap<>();
        BitSet bitSet = BitSet.newInstance();
        bitSet.set(0, 1);
        bitSets.put(label, bitSet);

        return new UnionNodeProperties(
            new BitIdMap(sparseLongArray, LabelInformation.from(bitSets), AllocationTracker.empty()),
            propertiesMap
        );
    }
}
