/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.api;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeSparseLongArray;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UnionNodePropertiesTest {

    @Test
    void shouldReturnDoubleCorrectly() {
        var unionNodeProperties = initializeUnionNodeProperties(Values.of(42.0D));

        assertThat(unionNodeProperties.doubleValue(0)).isEqualTo(42.0D);
    }

    @Test
    void shouldConvertLongToDouble() {
        var unionNodeProperties = initializeUnionNodeProperties(Values.of(42L));

        assertThat(unionNodeProperties.doubleValue(0)).isEqualTo(42.0D);
    }

    @Test
    void shouldThrowConversionException() {
        var unionNodeProperties = initializeUnionNodeProperties(Values.of(new double[]{1.337D}));

        assertThatThrownBy(() -> unionNodeProperties.doubleValue(0))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cannot safely convert DoubleArray")
            .hasMessageEndingWith("into a Double");
    }

    private UnionNodeProperties initializeUnionNodeProperties(Value propertyValue) {
        NodePropertiesFromStoreBuilder doubleNodePropertiesBuilder = NodePropertiesFromStoreBuilder.of(
            1, AllocationTracker.empty(), DefaultValue.of(0.0D)
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

        HugeSparseLongArray.Builder builder = HugeSparseLongArray.Builder.create(1, AllocationTracker.empty());
        builder.set(0, 0);

        HashMap<NodeLabel, BitSet> bitSets = new HashMap<>();
        BitSet bitSet = BitSet.newInstance();
        bitSet.set(0, 1);
        bitSets.put(label, bitSet);

        return new UnionNodeProperties(
            new IdMap(graphIds, builder.build(), sparseLongArray, bitSets, 1, AllocationTracker.empty()),
            propertiesMap
        );
    }
}
