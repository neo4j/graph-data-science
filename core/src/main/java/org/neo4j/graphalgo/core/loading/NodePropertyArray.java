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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeSparseLongArray;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.OptionalDouble;
import java.util.OptionalLong;

public final class NodePropertyArray implements NodeProperties {

    private static final MemoryEstimation MEMORY_ESTIMATION = MemoryEstimations
        .builder(NodePropertyArray.class)
        .rangePerGraphDimension(
            "property values",
            (dimensions, concurrency) -> HugeSparseLongArray.memoryEstimation(
                dimensions.nodeCount(),
                dimensions.nodeCount()
            )
        )
        .build();

    static MemoryEstimation memoryEstimation() {
        return MEMORY_ESTIMATION;
    }

    private final double defaultValue;
    private final OptionalLong longMaxValue;
    private final OptionalDouble doubleMaxValue;
    private final long size;
    private final HugeSparseLongArray properties;
    private final ValueType valueType;

    NodePropertyArray(
        ValueType valueType,
        double defaultValue,
        OptionalLong longMaxValue,
        OptionalDouble doubleMaxValue,
        long size,
        HugeSparseLongArray properties
    ) {
        this.valueType = valueType;
        this.defaultValue = defaultValue;
        this.longMaxValue = longMaxValue;
        this.doubleMaxValue = doubleMaxValue;
        this.size = size;
        this.properties = properties;
    }

    @Override
    public double getDouble(long nodeId) {
        return getDouble(nodeId, defaultValue);
    }

    @Override
    public double getDouble(long nodeId, double defaultValue) {
        var bits = properties.get(nodeId);
        // -1 is represented as NaN, but it's a different NaN than NaN,
        // so we can differentiate between explicit NaN and a missing value
        return bits == -1 ? defaultValue : Double.longBitsToDouble(bits);
    }

    @Override
    public long getLong(long nodeId) {
        return (long) getDouble(nodeId, defaultValue);
    }

    @Override
    public long getLong(long nodeId, long defaultValue) {
        return (long) getDouble(nodeId, defaultValue);
    }

    @Override
    public Value getValue(long nodeId) {
        return Values.doubleValue(getDouble(nodeId));
    }

    @Override
    public Object getObject(long nodeId) {
        if (getType() == ValueType.LONG) {
            return getLong(nodeId);
        } else {
            return getDouble(nodeId);
        }
    }

    @Override
    public Object getObject(long nodeId, Object defaultValue) {
        if (getType() == ValueType.LONG) {
            return getLong(nodeId, (long) defaultValue);
        } else {
            return getDouble(nodeId, (double) defaultValue);
        }
    }

    @Override
    public ValueType getType() {
        return valueType;
    }

    @Override
    public OptionalLong getLongMaxPropertyValue() {
        if (longMaxValue.isPresent()) {
            return longMaxValue;
        } else if (doubleMaxValue.isPresent()) {
           return OptionalLong.of(Double.valueOf(doubleMaxValue.getAsDouble()).longValue());
        } else {
            return OptionalLong.empty();
        }
    }

    @Override
    public OptionalDouble getDoubleMaxPropertyValue() {
        if (doubleMaxValue.isPresent()) {
            return doubleMaxValue;
        } else if (longMaxValue.isPresent()) {
            return OptionalDouble.of(Long.valueOf(longMaxValue.getAsLong()).doubleValue());
        } else {
            return OptionalDouble.empty();
        }
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long release() {
        // NOTE: HugeSparseLongArray doesn't release, maybe we should add this in the future
        return 0;
    }
}
